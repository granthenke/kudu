// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package org.apache.kudu.backup

import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths}

import com.google.protobuf.util.JsonFormat
import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import org.apache.kudu.backup.Backup.TableMetadataPB
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.kudu.spark.kudu.SparkUtil._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.yetus.audience.{InterfaceAudience, InterfaceStability}
import org.slf4j.{Logger, LoggerFactory}

@InterfaceAudience.Private
@InterfaceStability.Unstable
object KuduBackup {
  val log: Logger = LoggerFactory.getLogger(getClass)

  def run(options: KuduBackupOptions, session: SparkSession): Unit = {
    val context = new KuduContext(options.kuduMasterAddresses, session.sparkContext)
    val path = Paths.get(options.path)
    log.info(s"Backing up to path: $path")

    val client = context.syncClient

    options.tables.foreach { t =>
      val table = client.openTable(t)
      val tablePath = path.resolve(t)

      val rdd = new KuduBackupRDD(table, options, context, session.sparkContext)
      val df = session.sqlContext.createDataFrame(rdd, sparkSchema(table.getSchema))
      // TODO: Prefix path with the time? Maybe a backup "name" parameter defaulted to something?
      // TODO: Add partitionBy clause to match table partitions in some way?
      // TODO: Take parameter for the SaveMode
      val writer = df.write.mode(SaveMode.ErrorIfExists)
      // TODO: Restrict format option
      writer.format(options.format).save(tablePath.toString)

      val tableMetadata = TableMetadata.getTableMetadata(table, options)
      writeTableMetadata(tableMetadata, tablePath, session)
    }
  }

  private def writeTableMetadata(metadata: TableMetadataPB, path: Path, session: SparkSession): Unit = {
    val conf = session.sparkContext.hadoopConfiguration
    val hPath = new HPath(path.resolve(TableMetadata.metadataFileName).toString)
    val fs = hPath.getFileSystem(conf)
    val out = fs.create(hPath, false)
    val json = JsonFormat.printer().print(metadata)
    out.write(json.getBytes(StandardCharsets.UTF_8))
    out.flush()
    out.close()
  }

  def main(args: Array[String]): Unit = {
    val options = KuduBackupOptions.parse(args)
      .getOrElse(throw new IllegalArgumentException())

    val session = SparkSession.builder()
      .appName("Kudu Table Backup")
      .getOrCreate()

    run(options, session)
  }
}

