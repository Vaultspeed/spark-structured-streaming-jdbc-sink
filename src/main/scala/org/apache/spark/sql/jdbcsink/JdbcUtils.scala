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

import java.sql.{
  Connection,
  Driver,
  DriverManager,
  JDBCType,
  PreparedStatement,
  ResultSet,
  ResultSetMetaData,
  SQLException
}
import java.util.Locale

import org.apache.spark.TaskContext
import org.apache.spark.executor.InputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.{
  CaseInsensitiveMap,
  DateTimeUtils,
  GenericArrayData
}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{
  toJavaDate,
  toJavaTimestamp,
  SQLDate,
  SQLTimestamp
}
import org.apache.spark.sql.execution.datasources.jdbc.{
  DriverRegistry,
  DriverWrapper,
  JDBCOptions
}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.NextIterator

import scala.util.Try
import scala.util.control.NonFatal
import scala.collection.JavaConverters._

/** Util functions for JDBC tables.
  */
object JdbcUtils extends Logging {

  /** Returns a factory for creating connections to the given JDBC URL.
    *
    * @param options - JDBC options that contains url, table and other information.
    */
  def createConnectionFactory(options: JDBCOptions): () => Connection = {
    val driverClass: String = options.driverClass
    () => {
      DriverRegistry.register(driverClass)
      val driver: Driver = DriverManager.getDrivers.asScala
        .collectFirst {
          case d: DriverWrapper
              if d.wrapped.getClass.getCanonicalName == driverClass =>
            d
          case d if d.getClass.getCanonicalName == driverClass => d
        }
        .getOrElse {
          throw new IllegalStateException(
            s"Did not find registered driver with class $driverClass"
          )
        }
      driver.connect(options.url, options.asConnectionProperties)
    }
  }

  /** Returns true if the table already exists in the JDBC database.
    */
  def tableExists(conn: Connection, options: JDBCOptions): Boolean = {
    // Somewhat hacky, but there isn't a good way to identify whether a table exists for all
    // SQL database systems using JDBC meta data calls, considering "table" could also include
    // the database name. Query used to find table exists can be overridden by the dialects.
    Try {
      val statement =
        conn.prepareStatement(
          JdbcDialects
            .get(options.url)
            .getTableExistsQuery(
              options.parameters(JDBCOptions.JDBC_TABLE_NAME)
            )
        )
      try {
        statement.executeQuery()
      } finally {
        statement.close()
      }
    }.isSuccess
  }

  /** Drops a table from the JDBC database.
    */
  def dropTable(conn: Connection, table: String): Unit = {
    val statement = conn.createStatement
    try {
      statement.executeUpdate(s"DROP TABLE $table")
    } finally {
      statement.close()
    }
  }

  /** Truncates a table from the JDBC database without side effects.
    */
  def truncateTable(conn: Connection, options: JDBCOptions): Unit = {
    val statement = conn.createStatement
    try {
      statement.executeUpdate(
        JdbcDialects
          .get(options.url)
          .getTruncateQuery(options.parameters(JDBCOptions.JDBC_TABLE_NAME))
      )
    } finally {
      statement.close()
    }
  }

  /** Returns an Insert SQL statement for inserting a row into the target table via JDBC conn.
    */
  def getInsertStatement(
      table: String,
      rddSchema: StructType,
      tableSchema: Option[StructType],
      isCaseSensitive: Boolean,
      dialect: JdbcDialect
  ): String = {
    val columns = if (tableSchema.isEmpty) {
      rddSchema.fields.map(x => dialect.quoteIdentifier(x.name)).mkString(",")
    } else {
      val columnNameEquality = if (isCaseSensitive) {
        org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
      } else {
        org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
      }
      // The generated insert statement needs to follow rddSchema's column sequence and
      // tableSchema's column names. When appending data into some case-sensitive DBMSs like
      // PostgreSQL/Oracle, we need to respect the existing case-sensitive column names instead of
      // RDD column names for user convenience.
      val tableColumnNames = tableSchema.get.fieldNames
      rddSchema.fields
        .map { col =>
          val normalizedName = tableColumnNames
            .find(f => columnNameEquality(f, col.name))
            .getOrElse {
              throw new AnalysisException(
                s"""Column "${col.name}" not found in schema $tableSchema"""
              )
            }
          dialect.quoteIdentifier(normalizedName)
        }
        .mkString(",")
    }
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")
    s"INSERT INTO $table ($columns) VALUES ($placeholders)"
  }

  /** Retrieve standard jdbc types.
    *
    * @param dt The datatype (e.g. [[org.apache.spark.sql.types.StringType]])
    * @return The default JdbcType for this DataType
    */
  def getCommonJDBCType(dt: DataType): Option[JdbcType] = {
    dt match {
      case IntegerType => Option(JdbcType("INTEGER", java.sql.Types.INTEGER))
      case LongType    => Option(JdbcType("BIGINT", java.sql.Types.BIGINT))
      case DoubleType =>
        Option(JdbcType("DOUBLE PRECISION", java.sql.Types.DOUBLE))
      case FloatType   => Option(JdbcType("REAL", java.sql.Types.FLOAT))
      case ShortType   => Option(JdbcType("INTEGER", java.sql.Types.SMALLINT))
      case ByteType    => Option(JdbcType("BYTE", java.sql.Types.TINYINT))
      case BooleanType => Option(JdbcType("BIT(1)", java.sql.Types.BIT))
      case StringType  => Option(JdbcType("TEXT", java.sql.Types.CLOB))
      case BinaryType  => Option(JdbcType("BLOB", java.sql.Types.BLOB))
      case TimestampType =>
        Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
      case DateType => Option(JdbcType("DATE", java.sql.Types.DATE))
      case t: DecimalType =>
        Option(
          JdbcType(
            s"DECIMAL(${t.precision},${t.scale})",
            java.sql.Types.DECIMAL
          )
        )
      case _ => None
    }
  }

  def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
    dialect
      .getJDBCType(dt)
      .orElse(getCommonJDBCType(dt))
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
            toJavaTimestamp(row.get(pos, dataType).asInstanceOf[SQLTimestamp])
          )

      case DateType =>
        (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
          stmt.setDate(
            pos + 1,
            toJavaDate(row.get(pos, dataType).asInstanceOf[SQLDate])
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
  def saveInternalPartition(
      getConnection: () => Connection,
      table: String,
      iterator: Iterator[InternalRow],
      rddSchema: StructType,
      insertStmt: String,
      batchSize: Int,
      dialect: JdbcDialect,
      isolationLevel: Int
  ): Iterator[Byte] = {
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
      Iterator.empty
    } catch {
      case e: SQLException =>
        val cause = e.getNextException
        if (cause != null && e.getCause != cause) {
          if (e.getCause == null) {
            e.initCause(cause)
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
        }
        conn.close()
      } else {
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

  /** Compute the schema string for this RDD.
    */
  def schemaString(
      schema: StructType,
      sparkSession: SparkSession,
      url: String,
      createTableColumnTypes: Option[String] = None
  ): String = {
    val sb = new StringBuilder()
    val dialect = JdbcDialects.get(url)
    val userSpecifiedColTypesMap = createTableColumnTypes
      .map(parseUserSpecifiedCreateTableColumnTypes(schema, sparkSession, _))
      .getOrElse(Map.empty[String, String])
    schema.fields.foreach { field =>
      val name = dialect.quoteIdentifier(field.name)
      val typ = userSpecifiedColTypesMap
        .getOrElse(
          field.name,
          getJdbcType(field.dataType, dialect).databaseTypeDefinition
        )
      val nullable = if (field.nullable) "" else "NOT NULL"
      sb.append(s", $name $typ $nullable")
    }
    if (sb.length < 2) "" else sb.substring(2)
  }

  /** Parses the user specified createTableColumnTypes option value string specified in the same
    * format as create table ddl column types, and returns Map of field name and the data type to
    * use in-place of the default data type.
    */
  private def parseUserSpecifiedCreateTableColumnTypes(
      schema: StructType,
      sparkSession: SparkSession,
      createTableColumnTypes: String
  ): Map[String, String] = {
    def typeName(f: StructField): String = {
      // char/varchar gets translated to string type. Real data type specified by the user
      // is available in the field metadata as HIVE_TYPE_STRING
      if (f.metadata.contains(HIVE_TYPE_STRING)) {
        f.metadata.getString(HIVE_TYPE_STRING)
      } else {
        f.dataType.catalogString
      }
    }

    val userSchema = CatalystSqlParser.parseTableSchema(createTableColumnTypes)
    val nameEquality = sparkSession.sessionState.conf.resolver

    // checks duplicate columns in the user specified column types.
    SchemaUtils.checkColumnNameDuplication(
      userSchema.map(_.name),
      "in the createTableColumnTypes option value",
      nameEquality
    )

    // checks if user specified column names exist in the DataFrame schema
    userSchema.fieldNames.foreach { col =>
      schema.find(f => nameEquality(f.name, col)).getOrElse {
        throw new AnalysisException(
          s"createTableColumnTypes option column $col not found in schema " +
            schema.catalogString
        )
      }
    }

    val userSchemaMap = userSchema.fields.map(f => f.name -> typeName(f)).toMap
    val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
    if (isCaseSensitive) userSchemaMap else CaseInsensitiveMap(userSchemaMap)
  }

  /** Creates a table with a given schema.
    */
  def createTable(
      conn: Connection,
      schema: StructType,
      sparkSession: SparkSession,
      options: JDBCOptions
  ): Unit = {
    val strSchema = schemaString(
      schema,
      sparkSession,
      options.url,
      options.createTableColumnTypes
    )
    val table = options.parameters(JDBCOptions.JDBC_TABLE_NAME)
    val createTableOptions = options.createTableOptions
    // Create the table if the table does not exist.
    // To allow certain options to append when create a new table, which can be
    // table_options or partition_options.
    // E.g., "CREATE TABLE t (name string) ENGINE=InnoDB DEFAULT CHARSET=utf8"
    val sql = s"CREATE TABLE $table ($strSchema) $createTableOptions"
    val statement = conn.createStatement
    try {
      statement.executeUpdate(sql)
    } finally {
      statement.close()
    }
  }
}
