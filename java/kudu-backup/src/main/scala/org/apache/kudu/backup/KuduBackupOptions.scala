package org.apache.kudu.backup

import java.net.InetAddress

import org.apache.kudu.client.AsyncKuduClient
import org.apache.yetus.audience.{InterfaceAudience, InterfaceStability}
import scopt.OptionParser

@InterfaceAudience.Private
@InterfaceStability.Unstable
case class KuduBackupOptions(tables: Seq[String],
                             path: String,
                             kuduMasterAddresses: String = InetAddress.getLocalHost.getCanonicalHostName,
                             timestampMs: Long = System.currentTimeMillis(),
                             format: String = KuduBackupOptions.defaultFormat,
                             scanBatchSize: Int = KuduBackupOptions.defaultScanBatchSize,
                             scanRequestTimeout: Long = KuduBackupOptions.defaultScanRequestTimeout,
                             scanPrefetching: Boolean = KuduBackupOptions.defaultScanPrefetching)

object KuduBackupOptions {
  val defaultFormat: String = "parquet"
  val defaultScanBatchSize: Int = 1024*1024*20 // 20 MiB
  val defaultScanRequestTimeout: Long = AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS // 30 seconds
  val defaultScanPrefetching: Boolean = false // TODO: Add a test per KUDU-1260 and enable by default?

  // TODO: clean up usage output
  // TODO: timeout configurations
  private val parser: OptionParser[KuduBackupOptions] = new OptionParser[KuduBackupOptions]("KuduBackup") {
    opt[String]("path")
      .action((v, o) => o.copy(path = v))
      .text("The root path to output backup data.")
      .optional()

    opt[String]("kuduMasterAddresses")
      .action((v, o) => o.copy(kuduMasterAddresses = v))
      .text("Comma separated addresses of Kudu master nodes")
      .optional()

    opt[Long]("timestampMs")
      .action((v, o) => o.copy(timestampMs = v))
      .text("A UNIX timestamp in milliseconds since the epoch to execute scans at")
      .optional()

    opt[String]("format")
      .action((v, o) => o.copy(format = v))
      .text("The file format to use when writing the data.")
      .optional()

    opt[Int]("scanBatchSize")
      .action((v, o) => o.copy(scanBatchSize = v))
      .text("The maximum number of bytes returned by the scanner, on each batch")
      .optional()

    opt[Int]("scanRequestTimeout")
      .action((v, o) => o.copy(scanRequestTimeout = v))
      .text("Sets how long each scan request to a server can last")
      .optional()

    opt[Unit]("scanPrefetching")
      .action( (_, o) => o.copy(scanPrefetching = true) )
      .text("An experimental flag to enable pre-fetching data")
      .optional()

    arg[String]("<table>...")
      .unbounded()
      .action( (v, o) => o.copy(tables = o.tables :+ v) )
      .text("A list of tables to be backed up")
  }

  /**
    * Parses the passed arguments into Some[KuduBackupOptions].
    *
    * If the arguments are bad, an error message is displayed
    * and None is returned.
    *
    * @param args The arguments to parse.
    * @return Some[KuduBackupOptions] if parsing was successful, None if not.
    */
  def parse(args: Seq[String]): Option[KuduBackupOptions] = {
    parser.parse(args, KuduBackupOptions(Seq(), null))
  }
}