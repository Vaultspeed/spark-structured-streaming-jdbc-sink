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

import java.sql.Connection

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.jdbcsink.JdbcUtils._
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.streaming.OutputMode

class JdbcSink(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    partitionColumns: Seq[String],
    outputMode: OutputMode
) extends Sink
    with Logging {
  val options = new JDBCOptions(parameters)

  def addBatch(batchId: Long, df: DataFrame): Unit = {

    // NOTE: fail fast with not supported output mode
    if (
      outputMode != OutputMode.Complete() && outputMode != OutputMode.Append()
    ) {
      throw new IllegalArgumentException(s"$outputMode not supported")
    }

    val conn = JdbcUtils.createConnectionFactory(options)()
    try {
      val isCaseSensitive = sqlContext.conf.caseSensitiveAnalysis
      val tableExists = JdbcUtils.tableExists(conn, options)

      if (!tableExists) {
        createTable(conn, df.schema, df.sparkSession, options)
      }

      if (outputMode == OutputMode.Complete()) {
        if (
          options.isTruncate && JdbcDialects
            .get(options.url)
            .isCascadingTruncateTable()
            .contains(
              false
            )
        ) {
          // In this case, we should truncate table and then load.
          truncateTable(conn, options)
        } else {
          // Otherwise, do not truncate the table, instead drop and recreate it
          if (tableExists) {
            dropTable(conn, options.parameters(JDBCOptions.JDBC_TABLE_NAME))
            createTable(conn, df.schema, df.sparkSession, options)
          }
        }
      }

      saveRowsToTargetTable(df, isCaseSensitive, options, batchId)

    } finally {
      conn.close()
    }
  }

  /** Saves the RDD to the database in a single transaction.
    */
  def saveRowsToTargetTable(
      df: DataFrame,
      isCaseSensitive: Boolean,
      options: JDBCOptions,
      batchId: Long
  ): Unit = {
    val url = options.url
    val table = options.parameters(JDBCOptions.JDBC_TABLE_NAME)
    val insertStatement = options.parameters.get("insertStatement")
    val dialect = JdbcDialects.get(url)
    val getConnection: () => Connection = createConnectionFactory(options)
    val batchSize = options.batchSize
    val isolationLevel = options.isolationLevel

    val repartitionedDF = options.numPartitions match {
      case Some(n) if n <= 0 =>
        throw new IllegalArgumentException(
          s"Invalid value `$n` for parameter `${JDBCOptions.JDBC_NUM_PARTITIONS}` in table writing " +
            "via JDBC. The minimum value is 1."
        )
      case Some(n) if n < df.rdd.getNumPartitions => df.coalesce(n)
      case _                                      => df
    }

    val schema = df.schema
    val insertStmt = if (insertStatement.isDefined) {
      insertStatement.get
    } else {
      getInsertStatement(table, schema, None, isCaseSensitive, dialect)
    }

    repartitionedDF.queryExecution.toRdd.foreachPartition(iterator => {
      JdbcUtils.saveInternalPartition(
        getConnection,
        table,
        iterator,
        schema,
        insertStmt,
        batchSize,
        dialect,
        isolationLevel
      )
    })
  }

}
