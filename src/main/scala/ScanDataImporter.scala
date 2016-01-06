import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Put}
import org.slf4j.LoggerFactory

/**
  * Created by cmatta on 12/23/15.
  */
object ScanDataImporter {
  val logger = LoggerFactory.getLogger("ScanDataImporter")
  def main(args: Array[String]): Unit = {
    val tableName = args(0)
    val file = scala.io.Source.fromFile(args(1))
    val config: Configuration = HBaseConfiguration.create()
    val table: HTable = new HTable(config, tableName)
    val admin: HBaseAdmin = new HBaseAdmin(config)
    val rowTracker: scala.collection.mutable.Set[String] = scala.collection.mutable.Set[String]()
    var successCounter: Int = 0
    var recordCounter: Int = 0

    logger.info(s"Importing records into: $tableName")
    file.getLines.foreach{ line =>
      recordCounter += 1
      val rowKey = line.split(" ")(1)
      val column = line.split("column=")(1).split(",")(0).split(":")
      val family = column(0)
      val columnName = column(1)
      val timestamp = line.split("timestamp=")(1).split(",")(0).toLong
      var value: Option[String] = None
      if(line.split("value=").length > 1) {
        value = Some(line.split("value=")(1))
      }

      rowTracker add rowKey

      if(!familyExists(table, family)) {
        logger.warn(s"Column family $family doesn't exist!")
        println(s"Column family $family doesn't exist, create with defaults? [y/N]")
        if(!readBoolean()) System.exit(0)
        logger.warn(s"Creating column family $family with defaults.")
        admin.disableTable(table.getName)
        val columnDescriptor = new HColumnDescriptor(family.getBytes)
        admin.addColumn(table.getName, columnDescriptor)
        admin.enableTable(table.getName)
      }

      val put = new Put(rowKey.getBytes, timestamp)
      put.add(family.getBytes, columnName.getBytes, value.getOrElse("").getBytes)
      try {
        table.put(put)
        successCounter += 1
      } catch {
        case e: Exception =>
          println(s"$rowKey $family:$columnName $timestamp: " + value.getOrElse("no value"))
          println("Error! " + e.getMessage)

      }
    }
    println(s"Imported $successCounter of $recordCounter records into table: $table totalling " +
      rowTracker.toList.length + " rows."
    )
  }

  def familyExists(table: HTable, family: String): Boolean = table.getTableDescriptor.hasFamily(family.getBytes)
}
