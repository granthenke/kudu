package org.apache.kudu.backup

import java.net.InetAddress

import org.apache.yetus.audience.{InterfaceAudience, InterfaceStability}
import scopt.OptionParser

@InterfaceAudience.Private
@InterfaceStability.Unstable
case class KuduRestoreOptions(tables: Seq[String],
                              path: String,
                              kuduMasterAddresses: String = InetAddress.getLocalHost.getCanonicalHostName,
                              tableSuffix: String = KuduRestoreOptions.defaultTableSuffix,
                              createTables: Boolean = KuduRestoreOptions.defaultCreateTables,
                              metadataPath: String = "",
                              format: String = KuduRestoreOptions.defaultFormat)

object KuduRestoreOptions {
  val defaultFormat: String = "parquet"
  val defaultTableSuffix: String = "-restore"
  val defaultCreateTables: Boolean = true

  // TODO: clean up usage output
  // TODO: timeout configurations
  private val parser: OptionParser[KuduRestoreOptions] = new OptionParser[KuduRestoreOptions]("KuduRestore") {
    opt[String]("path")
      .action((v, o) => o.copy(path = v))
      .text("The root path to the backup data.")
      .optional()

    opt[String]("kuduMasterAddresses")
      .action((v, o) => o.copy(kuduMasterAddresses = v))
      .text("Comma separated addresses of Kudu master nodes")
      .optional()

    opt[String]("tableSuffix")
      .action((v, o) => o.copy(tableSuffix = v))
      .text("The suffix to add to the restored table names")
      .optional()

    opt[Boolean]("createTables")
      .action( (v, o) => o.copy(createTables = v) )
      .text("true if you would like to create the target tables, false if they already exist")
      .optional()

    opt[String]("metadataPath")
      .action((v, o) => o.copy(metadataPath = v))
      .text("The root path to look for table metadata. Useful when overriding metadata of restored tables")
      .optional()

    opt[String]("format")
      .action((v, o) => o.copy(format = v))
      .text("The file format to use when reading the data.")
      .optional()

    arg[String]("<table>...")
      .unbounded()
      .action( (v, o) => o.copy(tables = o.tables :+ v) )
      .text("A list of tables to be restored")
  }

  /**
    * Parses the passed arguments into Some[KuduRestoreOptions].
    *
    * If the arguments are bad, an error message is displayed
    * and None is returned.
    *
    * @param args The arguments to parse.
    * @return Some[KuduRestoreOptions] if parsing was successful, None if not.
    */
  def parse(args: Seq[String]): Option[KuduRestoreOptions] = {
    parser.parse(args, KuduRestoreOptions(Seq(), null))
  }
}