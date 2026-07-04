/*
 * Copyright 2024 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oceanbase.spark

import com.oceanbase.spark.OceanBaseCatalogE2eITCase.{MYSQL_CONNECTOR_JAVA, SINK_CONNECTOR_NAME, SPARSE_IDS, SPARSE_PARTITION_PRODUCTS}
import com.oceanbase.spark.OceanBaseTestBase.assertEqualsInAnyOrder
import com.oceanbase.spark.utils.SparkContainerTestEnvironment
import com.oceanbase.spark.utils.SparkContainerTestEnvironment.getResource

import org.junit.jupiter.api._
import org.junit.jupiter.api.condition.DisabledIfSystemProperty
import org.junit.jupiter.api.function.ThrowingSupplier
import org.slf4j.LoggerFactory
import org.testcontainers.containers.output.Slf4jLogConsumer

import java.sql.SQLException
import java.util

import scala.collection.mutable
import scala.util.matching.Regex

class OceanBaseCatalogE2eITCase extends SparkContainerTestEnvironment {
  private val LOG = LoggerFactory.getLogger(classOf[OceanBaseCatalogE2eITCase])

  @BeforeEach
  @throws[Exception]
  override def before(): Unit = {
    super.before()
    initialize("sql/mysql/products.sql")
  }

  @AfterEach
  @throws[Exception]
  override def after(): Unit = {
    super.after()
    dropTables("products")
  }

  @Test
  @DisabledIfSystemProperty(
    named = "spark_version",
    matches = "^2\\.4\\.[0-9]$",
    disabledReason = "Catalog is only supported starting from Spark3."
  )
  def testInsertValues(): Unit = {
    val sqlLines: util.List[String] = new util.ArrayList[String]
    sqlLines.add(s"""
                    |set spark.sql.catalog.ob=com.oceanbase.spark.catalog.OceanBaseCatalog;
                    |set spark.sql.catalog.ob.url=$getJdbcUrlInContainer;
                    |set spark.sql.catalog.ob.username=$getUsername;
                    |set spark.sql.catalog.ob.password=$getPassword;
                    |set `spark.sql.catalog.ob.schema-name`=$getSchemaName;
                    |set spark.sql.defaultCatalog=ob;
                    |""".stripMargin)
    sqlLines.add(
      s"""
         |INSERT INTO $getSchemaName.products VALUES
         |(101, 'scooter', 'Small 2-wheel scooter', 3.14),
         |(102, 'car battery', '12V car battery', 8.1),
         |(103, '12-pack drill bits', '12-pack of drill bits with sizes ranging from #40 to #3', 0.8),
         |(104, 'hammer', '12oz carpenter\\'s hammer', 0.75),
         |(105, 'hammer', '14oz carpenter\\'s hammer', 0.875),
         |(106, 'hammer', '16oz carpenter\\'s hammer', 1.0),
         |(107, 'rocks', 'box of assorted rocks', 5.3),
         |(108, 'jacket', 'water resistent black wind breaker', 0.1),
         |(109, 'spare tire', '24 inch spare tire', 22.2);
         |""".stripMargin)

    sqlLines.add("select * from products limit 10;")

    submitSQLJob(sqlLines, getResource(SINK_CONNECTOR_NAME), getResource(MYSQL_CONNECTOR_JAVA))

    val expected: util.List[String] = util.Arrays.asList(
      "101,scooter,Small 2-wheel scooter,3.1400000000",
      "102,car battery,12V car battery,8.1000000000",
      "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8000000000",
      "104,hammer,12oz carpenter's hammer,0.7500000000",
      "105,hammer,14oz carpenter's hammer,0.8750000000",
      "106,hammer,16oz carpenter's hammer,1.0000000000",
      "107,rocks,box of assorted rocks,5.3000000000",
      "108,jacket,water resistent black wind breaker,0.1000000000",
      "109,spare tire,24 inch spare tire,22.2000000000"
    )
    val actual: util.List[String] = queryTable("products")
    assertEqualsInAnyOrder(expected, actual)
  }

  @Test
  @DisabledIfSystemProperty(
    named = "spark_version",
    matches = "^2\\.4\\.[0-9]$",
    disabledReason = "Catalog is only supported starting from Spark3."
  )
  def testDirectLoad(): Unit = {
    val sqlLines: util.List[String] = new util.ArrayList[String]
    sqlLines.add(s"""
                    |set spark.sql.catalog.ob=com.oceanbase.spark.catalog.OceanBaseCatalog;
                    |set spark.sql.catalog.ob.url=$getJdbcUrlInContainer;
                    |set spark.sql.catalog.ob.username=$getUsername;
                    |set spark.sql.catalog.ob.password=$getPassword;
                    |set `spark.sql.catalog.ob.schema-name`=$getSchemaName;
                    |set `spark.sql.catalog.ob.direct-load.enabled`=true;
                    |set `spark.sql.catalog.ob.direct-load.host`=$getHostInContainer;
                    |set `spark.sql.catalog.ob.direct-load.rpc-port`=$getRpcPortInContainer;
                    |set spark.sql.defaultCatalog=ob;
                    |""".stripMargin)
    sqlLines.add(
      s"""
         |INSERT INTO $getSchemaName.products VALUES
         |(101, 'scooter', 'Small 2-wheel scooter', 3.14),
         |(102, 'car battery', '12V car battery', 8.1),
         |(103, '12-pack drill bits', '12-pack of drill bits with sizes ranging from #40 to #3', 0.8),
         |(104, 'hammer', '12oz carpenter\\'s hammer', 0.75),
         |(105, 'hammer', '14oz carpenter\\'s hammer', 0.875),
         |(106, 'hammer', '16oz carpenter\\'s hammer', 1.0),
         |(107, 'rocks', 'box of assorted rocks', 5.3),
         |(108, 'jacket', 'water resistent black wind breaker', 0.1),
         |(109, 'spare tire', '24 inch spare tire', 22.2);
         |""".stripMargin)

    sqlLines.add("select * from products limit 10;")

    submitSQLJob(sqlLines, getResource(SINK_CONNECTOR_NAME), getResource(MYSQL_CONNECTOR_JAVA))

    val expected: util.List[String] = util.Arrays.asList(
      "101,scooter,Small 2-wheel scooter,3.1400000000",
      "102,car battery,12V car battery,8.1000000000",
      "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8000000000",
      "104,hammer,12oz carpenter's hammer,0.7500000000",
      "105,hammer,14oz carpenter's hammer,0.8750000000",
      "106,hammer,16oz carpenter's hammer,1.0000000000",
      "107,rocks,box of assorted rocks,5.3000000000",
      "108,jacket,water resistent black wind breaker,0.1000000000",
      "109,spare tire,24 inch spare tire,22.2000000000"
    )
    val actual: util.List[String] = queryTable("products")
    assertEqualsInAnyOrder(expected, actual)
  }

  @Test
  @DisabledIfSystemProperty(
    named = "spark_version",
    matches = "^2\\.4\\.[0-9]$",
    disabledReason = "Catalog is only supported starting from Spark3."
  )
  def testReadWithMultipleJdbcUrls(): Unit = {
    val sqlLines: util.List[String] = new util.ArrayList[String]
    sqlLines.add(
      s"""
         |set spark.sql.catalog.ob=com.oceanbase.spark.catalog.OceanBaseCatalog;
         |set spark.sql.catalog.ob.url=$getUnavailableJdbcUrlInContainer,$getJdbcUrlInContainer;
         |set spark.sql.catalog.ob.username=$getUsername;
         |set spark.sql.catalog.ob.password=$getPassword;
         |set `spark.sql.catalog.ob.schema-name`=$getSchemaName;
         |set `spark.sql.catalog.ob.jdbc.connection.max-retries`=1;
         |set `spark.sql.catalog.ob.jdbc.connection.retry-interval`=PT0S;
         |set `spark.sql.catalog.ob.jdbc.connection.failed-url-cooldown`=PT1S;
         |set spark.sql.defaultCatalog=ob;
         |""".stripMargin)
    sqlLines.add(s"select id, name from $getSchemaName.products where id = 101;")

    submitSQLJob(sqlLines, getResource(SINK_CONNECTOR_NAME), getResource(MYSQL_CONNECTOR_JAVA))
  }

  @Test
  @DisabledIfSystemProperty(
    named = "spark_version",
    matches = "^2\\.4\\.[0-9]$",
    disabledReason = "Catalog is only supported starting from Spark3."
  )
  def testApproximateRowCountPartitionPlanning(): Unit = {
    createSparsePartitionProducts()
    try {
      val sqlLines: util.List[String] = new util.ArrayList[String]
      sqlLines.add(s"""
                      |set spark.sql.catalog.ob=com.oceanbase.spark.catalog.OceanBaseCatalog;
                      |set spark.sql.catalog.ob.url=$getJdbcUrlInContainer;
                      |set spark.sql.catalog.ob.username=$getUsername;
                      |set spark.sql.catalog.ob.password=$getPassword;
                      |set `spark.sql.catalog.ob.schema-name`=$getSchemaName;
                      |set `spark.sql.catalog.ob.jdbc.use-approximate-row-count`=true;
                      |set `spark.sql.catalog.ob.jdbc.max-records-per-partition`=2;
                      |set `spark.sql.catalog.ob.jdbc.num-partitions`=4;
                      |set `spark.sql.catalog.ob.jdbc.bucket-multiplier`=2;
                      |set spark.sql.defaultCatalog=ob;
                      |""".stripMargin)
      sqlLines.add(
        s"""
           |SELECT concat(cast(id AS string), ':', cast(spark_partition_id() AS string)) AS marker
           |FROM $getSchemaName.$SPARSE_PARTITION_PRODUCTS;
           |""".stripMargin)

      val result =
        submitSQLJobWithResult(
          sqlLines,
          getResource(SINK_CONNECTOR_NAME),
          getResource(MYSQL_CONNECTOR_JAVA))
      Assertions.assertEquals(0, result.getExitCode)

      val combinedOutput = result.getStdout + "\n" + result.getStderr
      val markers = extractPartitionMarkers(combinedOutput)
      Assertions.assertEquals(SPARSE_IDS.toSet, markers.keySet)
      Assertions.assertTrue(
        markers.values.toSet.size > 1,
        s"Expected bucket partition planning to use multiple Spark partitions, markers=$markers")

      val capturedSql = captureSqlAudit()
      if (capturedSql.nonEmpty) {
        val executedSql = capturedSql.mkString("\n")
        Assertions.assertTrue(
          executedSql.toLowerCase.contains("information_schema.partitions"),
          s"Expected partition planning to read approximate row counts from information_schema.partitions. SQL=$executedSql"
        )
        Assertions.assertFalse(
          exactCountQueryPattern.findFirstIn(executedSql).isDefined,
          s"Partition planning must not execute SELECT count(1) when approximate row count is enabled. SQL=$executedSql"
        )
      }
    } finally {
      dropTableIfExists(SPARSE_PARTITION_PRODUCTS)
      disableSqlAudit()
    }
  }

  @Test
  @DisabledIfSystemProperty(
    named = "spark_version",
    matches = "^2\\.4\\.[0-9]$",
    disabledReason = "Catalog is only supported starting from Spark3."
  )
  def testCatalogOp(): Unit = {
    val sqlLines: util.List[String] = new util.ArrayList[String]
    sqlLines.add(s"""
                    |set spark.sql.catalog.ob=com.oceanbase.spark.catalog.OceanBaseCatalog;
                    |set spark.sql.catalog.ob.url=$getJdbcUrlInContainer;
                    |set spark.sql.catalog.ob.username=$getUsername;
                    |set spark.sql.catalog.ob.password=$getPassword;
                    |set `spark.sql.catalog.ob.schema-name`=$getSchemaName;
                    |set spark.sql.defaultCatalog=ob;
                    |""".stripMargin)

    sqlLines.add(s"""
                    |show databases;
                    |show tables;
                    |use test;
                    |select * from products limit 10;
                    |""".stripMargin)

    Assertions.assertDoesNotThrow(new ThrowingSupplier[Unit] {
      override def get(): Unit = {
        submitSQLJob(sqlLines, getResource(SINK_CONNECTOR_NAME), getResource(MYSQL_CONNECTOR_JAVA))
      }
    })
  }

  private def getUnavailableJdbcUrlInContainer: String =
    s"jdbc:mysql://127.0.0.1:1/$getSchemaName?useUnicode=true&characterEncoding=UTF-8&useSSL=false"

  private def createSparsePartitionProducts(): Unit = {
    resetSqlAudit()
    val connection = getJdbcConnection
    try {
      val statement = connection.createStatement()
      try {
        statement.execute(s"DROP TABLE IF EXISTS $SPARSE_PARTITION_PRODUCTS")
        statement.execute(s"""
                             |CREATE TABLE $SPARSE_PARTITION_PRODUCTS (
                             |  id BIGINT NOT NULL PRIMARY KEY,
                             |  name VARCHAR(64) NOT NULL
                             |)
                             |""".stripMargin)
        SPARSE_IDS.foreach {
          id =>
            statement.addBatch(
              s"INSERT INTO $SPARSE_PARTITION_PRODUCTS(id, name) VALUES ($id, 'name_$id')")
        }
        statement.executeBatch()
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }

  private def resetSqlAudit(): Unit = {
    try {
      executeSqlAuditStatement("SET GLOBAL log_output = 'TABLE'")
      executeSqlAuditStatement("SET GLOBAL general_log = 'OFF'")
      executeSqlAuditStatement("TRUNCATE TABLE mysql.general_log")
      executeSqlAuditStatement("SET GLOBAL general_log = 'ON'")
    } catch {
      case e: SQLException =>
        LOG.warn(
          "SQL audit through mysql.general_log is not available; skipping SQL-path assertions",
          e)
    }
  }

  private def disableSqlAudit(): Unit = {
    try {
      executeSqlAuditStatement("SET GLOBAL general_log = 'OFF'")
    } catch {
      case e: SQLException =>
        LOG.warn("Failed to disable mysql.general_log after E2E test", e)
    }
  }

  private def executeSqlAuditStatement(sql: String): Unit = {
    val connection = getJdbcConnection
    try {
      val statement = connection.createStatement()
      try {
        statement.execute(sql)
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }

  private def captureSqlAudit(): Seq[String] = {
    val connection = getJdbcConnection
    try {
      val statement = connection.createStatement()
      try {
        val rs = statement.executeQuery(
          s"""
             |SELECT argument
             |FROM mysql.general_log
             |WHERE command_type = 'Query'
             |  AND (
             |       lower(argument) LIKE '%${SPARSE_PARTITION_PRODUCTS.toLowerCase}%'
             |    OR lower(argument) LIKE '%information_schema.partitions%'
             |  )
             |""".stripMargin)
        val queries = mutable.ArrayBuffer[String]()
        while (rs.next()) {
          queries += rs.getString(1)
        }
        queries.toSeq
      } finally {
        statement.close()
      }
    } catch {
      case e: SQLException =>
        LOG.warn("Failed to read mysql.general_log; skipping SQL-path assertions", e)
        Seq.empty
    } finally {
      connection.close()
    }
  }

  private def dropTableIfExists(tableName: String): Unit = {
    val connection = getJdbcConnection
    try {
      val statement = connection.createStatement()
      try {
        statement.execute(s"DROP TABLE IF EXISTS $tableName")
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }

  private def extractPartitionMarkers(output: String): Map[Long, Int] = {
    val markerPattern: Regex = """^\s*\|?\s*(\d+):(\d+)\s*\|?\s*$""".r
    val markers = mutable.LinkedHashMap.empty[Long, Int]
    output.linesIterator.foreach {
      case markerPattern(idText, partitionIdText) =>
        val id = idText.toLong
        if (SPARSE_IDS.contains(id)) markers.put(id, partitionIdText.toInt)
      case _ =>
    }
    markers.toMap
  }

  private val exactCountQueryPattern: Regex =
    s"(?is)select\\s*/\\*.*?\\*/\\s*count\\s*\\(\\s*1\\s*\\).*$SPARSE_PARTITION_PRODUCTS".r
}

object OceanBaseCatalogE2eITCase extends SparkContainerTestEnvironment {
  private val LOG = LoggerFactory.getLogger(classOf[OceanBaseE2eITCase])
  val SPARSE_PARTITION_PRODUCTS = "sparse_partition_products"
  val SPARSE_IDS = Seq(1L, 1000L, 2000L, 100000L, 100001L, 500000L, 900000L, 2000000L, 2000001L,
    5000000L, 9000000L, 9000001L)
  private val SINK_CONNECTOR_NAME =
    "^.*spark-connector-oceanbase-\\d+\\.\\d+_\\d+\\.\\d+-[\\d\\.]+(?:-SNAPSHOT)?\\.jar$"
  private val MYSQL_CONNECTOR_JAVA = "mysql-connector-java.jar"

  @BeforeAll def setup(): Unit = {
    OceanBaseMySQLTestBase.CONTAINER.withLogConsumer(new Slf4jLogConsumer(LOG)).start()
  }

  @AfterAll def tearDown(): Unit = {
    OceanBaseMySQLTestBase.CONTAINER.stop()
  }

}
