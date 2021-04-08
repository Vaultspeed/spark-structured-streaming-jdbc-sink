/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.jdbcsink

import java.sql.{Connection, PreparedStatement, SQLException}

import scala.util.control.NonFatal

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{
  toJavaDate,
  toJavaTimestamp
}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._

/** Util functions for JDBC tables.
  */
object JdbcUtilsInternal extends Logging {

  // NOTE: private in original JdbcUtils
  def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
    dialect
      .getJDBCType(dt)
      .orElse(JdbcUtils.getCommonJDBCType(dt))
      .getOrElse(
        throw new IllegalArgumentException(
          s"Can't get JDBC type for ${dt.simpleString}"
        )
      )
  }

  // A `JDBCInternalValueSetter` is responsible for setting a value from `InternalRow`
  // into a field for `PreparedStatement`. The last argument `Int` means the index for
  // the value to be set in the SQL statement and also used for the value in `Row`.
  private type JDBCInternalValueSetter =
    (PreparedStatement, InternalRow, Int) => Unit

  def makeInternalSetter(
      conn: Connection,
      dialect: JdbcDialect,
      dataType: DataType
  ): JDBCInternalValueSetter =
    dataType match {
      case IntegerType =>
        (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
          stmt.setInt(pos + 1, row.getInt(pos))

      case LongType =>
        (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
          stmt.setLong(pos + 1, row.getLong(pos))

      case DoubleType =>
        (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
          stmt.setDouble(pos + 1, row.getDouble(pos))

      case FloatType =>
        (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
          stmt.setFloat(pos + 1, row.getFloat(pos))

      case ShortType =>
        (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
          stmt.setInt(pos + 1, row.getShort(pos))

      case ByteType =>
        (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
          stmt.setInt(pos + 1, row.getByte(pos))

      case BooleanType =>
        (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
          stmt.setBoolean(pos + 1, row.getBoolean(pos))

      case StringType =>
        (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
          stmt.setString(pos + 1, row.getString(pos))

      case BinaryType =>
        (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
          stmt.setBytes(pos + 1, row.getBinary(pos))

      case TimestampType =>
        (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
          stmt.setTimestamp(
            pos + 1,
            toJavaTimestamp(row.get(pos, dataType).asInstanceOf[Long])
          )

      case DateType =>
        (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
          stmt.setDate(
            pos + 1,
            toJavaDate(row.get(pos, dataType).asInstanceOf[Int])
          )

      case t: DecimalType =>
        (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
          stmt.setBigDecimal(
            pos + 1,
            row.getDecimal(pos, 20, 4).toJavaBigDecimal
          )

      case ArrayType(et, _) =>
        // remove type length parameters from end of type name
        val typeName =
          getJdbcType(et, dialect).databaseTypeDefinition.toLowerCase
            .split("\\(")(0)
        (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
          val array =
            conn.createArrayOf(typeName, row.getArray(pos).toObjectArray(et))
          stmt.setArray(pos + 1, array)

      case _ =>
        (_: PreparedStatement, _: InternalRow, pos: Int) =>
          throw new IllegalArgumentException(
            s"Can't translate non-null value for field $pos"
          )
    }

  /** Saves a partition of a DataFrame to the JDBC database.  This is done in
    * a single database transaction (unless isolation level is "NONE")
    * in order to avoid repeatedly inserting data as much as possible.
    *
    * It is still theoretically possible for rows in a DataFrame to be
    * inserted into the database more than once if a stage somehow fails after
    * the commit occurs but before the stage can return successfully.
    *
    * This is not a closure inside saveTable() because apparently cosmetic
    * implementation changes elsewhere might easily render such a closure
    * non-Serializable.  Instead, we explicitly close over all variables that
    * are used.
    */
  def savePartition(
      getConnection: () => Connection,
      table: String,
      iterator: Iterator[InternalRow],
      rddSchema: StructType,
      insertStmt: String,
      batchSize: Int,
      dialect: JdbcDialect,
      isolationLevel: Int,
      options: JDBCOptions
  ): Unit = {

    val outMetrics = TaskContext.get().taskMetrics().outputMetrics
    val conn = getConnection()
    var committed = false

    var finalIsolationLevel = Connection.TRANSACTION_NONE
    if (isolationLevel != Connection.TRANSACTION_NONE) {
      try {
        val metadata = conn.getMetaData
        if (metadata.supportsTransactions()) {
          // Update to at least use the default isolation, if any transaction level
          // has been chosen and transactions are supported
          val defaultIsolation = metadata.getDefaultTransactionIsolation
          finalIsolationLevel = defaultIsolation
          if (metadata.supportsTransactionIsolationLevel(isolationLevel)) {
            // Finally update to actually requested level if possible
            finalIsolationLevel = isolationLevel
          } else {
            logWarning(
              s"Requested isolation level $isolationLevel is not supported; " +
                s"falling back to default isolation level $defaultIsolation"
            )
          }
        } else {
          logWarning(
            s"Requested isolation level $isolationLevel, but transactions are unsupported"
          )
        }
      } catch {
        case NonFatal(e) =>
          logWarning("Exception while detecting transaction support", e)
      }
    }
    val supportsTransactions =
      finalIsolationLevel != Connection.TRANSACTION_NONE
    var totalRowCount = 0L

    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
        conn.setTransactionIsolation(finalIsolationLevel)
      }
      val stmt = conn.prepareStatement(insertStmt)
      val setters =
        rddSchema.fields.map(f => makeInternalSetter(conn, dialect, f.dataType))
      val nullTypes =
        rddSchema.fields.map(f => getJdbcType(f.dataType, dialect).jdbcNullType)
      val numFields = rddSchema.fields.length

      try {
        var rowCount = 0
        while (iterator.hasNext) {
          val row = iterator.next()
          var i = 0
          while (i < numFields) {
            if (row.isNullAt(i)) {
              stmt.setNull(i + 1, nullTypes(i))
            } else {
              setters(i).apply(stmt, row, i)
            }
            i = i + 1
          }
          stmt.addBatch()
          rowCount += 1
          totalRowCount += 1
          if (rowCount % batchSize == 0) {
            stmt.executeBatch()
            rowCount = 0
          }
        }
        if (rowCount > 0) {
          stmt.executeBatch()
        }
      } finally {
        stmt.close()
      }
      if (supportsTransactions) {
        conn.commit()
      }
      committed = true
    } catch {
      case e: SQLException =>
        val cause = e.getNextException
        if (cause != null && e.getCause != cause) {
          // If there is no cause already, set 'next exception' as cause. If cause is null,
          // it *may* be because no cause was set yet
          if (e.getCause == null) {
            try {
              e.initCause(cause)
            } catch {
              // Or it may be null because the cause *was* explicitly initialized, to *null*,
              // in which case this fails. There is no other way to detect it.
              // addSuppressed in this case as well.
              case _: IllegalStateException => e.addSuppressed(cause)
            }
          } else {
            e.addSuppressed(cause)
          }
        }
        throw e
    } finally {
      if (!committed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through unless rollback() or close() want to
        // tell the user about another problem.
        if (supportsTransactions) {
          conn.rollback()
        } else {
          outMetrics.setRecordsWritten(totalRowCount)
        }
        conn.close()
      } else {
        outMetrics.setRecordsWritten(totalRowCount)

        // The stage must succeed.  We cannot propagate any exception close() might throw.
        try {
          conn.close()
        } catch {
          case e: Exception =>
            logWarning("Transaction succeeded, but closing failed", e)
        }
      }
    }
  }

}
