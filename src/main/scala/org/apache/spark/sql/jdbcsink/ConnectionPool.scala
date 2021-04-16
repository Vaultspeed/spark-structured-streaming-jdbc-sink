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

import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import com.zaxxer.hikari.{HikariDataSource, HikariConfig}

object ConnectionPool {

  private val pools = new ConcurrentHashMap[String, HikariDataSource]()

  val configPrefix = "connection."

  def getParametersStarting(
      parameters: Map[String, String],
      keyNameStart: String
  ): Properties = {
    val props = new Properties()
    parameters.filterKeys(key => key.startsWith(keyNameStart)).foreach {
      case (k, v) =>
        props.setProperty(
          snakeCaseToCamelCase(k.substring(keyNameStart.length())),
          v
        )
    }
    props
  }

  def snakeCaseToCamelCase(snake_words: String): String = {
    val parts = snake_words.split("_")
    parts.head + parts.tail.map((s) => firstToUpper(s)).mkString
  }

  def firstToUpper(word: String): String = {
    word.head.toString().toUpperCase() + word.tail
  }

  def getConfig(parameters: Map[String, String]): HikariConfig = {
    // NOTE: dataSource.something property would invoke config.addDataSourceProperty("something", "val")
    val config = new HikariConfig(
      getParametersStarting(parameters, configPrefix)
    )
    // NOTE: url option sets connection.jdbcUrl option
    if (parameters.get("url").isDefined && config.getJdbcUrl() == null) {
      config.setJdbcUrl(parameters("url"))
    }
    config
  }

  def getID(parameters: Map[String, String]): String = {
    parameters.getOrElse("url", "") +
      parameters.getOrElse("user", "") +
      parameters.getOrElse(configPrefix + "poolName", "") +
      parameters.getOrElse(configPrefix + "dataSourceClassName", "") +
      parameters.getOrElse(configPrefix + "jdbcUrl", "") +
      parameters.getOrElse(configPrefix + "catalog", "") +
      parameters.getOrElse(configPrefix + "username", "") +
      parameters.getOrElse(configPrefix + "autoCommit", "") +
      parameters.getOrElse(configPrefix + "connectionTimeout", "") +
      parameters.getOrElse(configPrefix + "idleTimeout", "") +
      parameters.getOrElse(configPrefix + "keepaliveTime", "") +
      parameters.getOrElse(configPrefix + "maxLifetime", "") +
      parameters.getOrElse(configPrefix + "minimumIdle", "") +
      parameters.getOrElse(configPrefix + "maximumPoolSize", "") +
      parameters.getOrElse(configPrefix + "transactionIsolation", "") +
      parameters.getOrElse(configPrefix + "connectionTimeout", "")
  }

  def get(parameters: Map[String, String]): HikariDataSource = {
    pools.computeIfAbsent(
      getID(parameters),
      (k) => new HikariDataSource(getConfig(parameters))
    )
  }

}
