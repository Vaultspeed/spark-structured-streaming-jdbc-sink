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

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
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

  def addBatch(batchId: Long, df: DataFrame): Unit = {

    // NOTE: fail fast with not supported output mode
    if (
      outputMode != OutputMode.Complete() &&
      outputMode != OutputMode.Append()
    ) {
      throw new IllegalArgumentException(
        s"$outputMode not supported by JdbcSink."
      )
    }

    val options = new JdbcOptionsInWrite(parameters)
    val ds = ConnectionPool.get(parameters)
    val conn = ds.getConnection

    try {

      val dialect = JdbcDialects.get(options.url)
      val tableName = options.table
      val isCaseSensitive = sqlContext.conf.caseSensitiveAnalysis
      var tableExists = JdbcUtils.tableExists(conn, options)
      val batchSize = options.batchSize
      val schema = df.schema

      val stmt = options.parameters.get("insertStatement")
      val insertStmt = if (stmt.isDefined) { stmt.get }
      else {
        JdbcUtils.getInsertStatement(
          tableName,
          schema,
          None,
          isCaseSensitive,
          dialect
        )
      }

      if (outputMode == OutputMode.Complete() && tableExists) {
        if (
          options.isTruncate && dialect
            .isCascadingTruncateTable()
            .contains(
              false
            )
        ) {
          JdbcUtils.truncateTable(conn, options)
        } else {
          JdbcUtils.dropTable(conn, tableName, options)
          tableExists = false
        }
      }

      if (!tableExists) {
        JdbcUtils.createTable(
          conn,
          tableName,
          schema,
          isCaseSensitive,
          options
        )
      }

      val repartitionedDF = options.numPartitions match {
        case Some(n) if n <= 0 =>
          throw new IllegalArgumentException(
            s"Invalid value `$n` for parameter `${JDBCOptions.JDBC_NUM_PARTITIONS}` in table writing " +
              "via JDBC. The minimum value is 1."
          )
        case Some(n) if n < df.rdd.getNumPartitions => df.coalesce(n)
        case _                                      => df
      }

      repartitionedDF.queryExecution.toRdd.foreachPartition(iterator =>
        JdbcUtilsInternal.savePartition(
          iterator,
          schema,
          insertStmt,
          batchSize,
          dialect,
          options
        )
      )
    } finally {
      conn.close()
    }
  }

}
