package org.custom.etl.data

import org.custom.etl.utils.{SparkConnector, LogHelper}
import org.custom.etl.utils.Services._
import org.custom.etl.utils.CustomUDFs._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.custom.etl.utils.{MSSQL, AVRODATAFILE, PARQUETDATAFILE}
import org.apache.spark.sql.SaveMode
import org.custom.etl.models.CommandLineConfig


object Main extends LogHelper {
  
  val sqlContext = SparkConnector.sqlContext
  
  private val DEFAULT_FILE = "test.txt"
  private val DEFAULT_WAREHOUSE_DIRECTORY = "user/hive/warehouse/spark_etl"
  private val DEFAULT_FILE_FORMAT = AVRODATAFILE
  
  /*
   * @deprecated  This is was part of the first version. This has been deprecated
  val connectionObject = ConnectionObject(host, user, password, Some(port))
  private[this] val connectionProfile = buildConnectionProfile(connectionObject, MSSQL)
  * 
  */
  
  val parser = new scopt.OptionParser[CommandLineConfig]("scopt") {
    head("VGG_SPARK_ETL", "1.0-SNAPSHOT")
    
    opt[String]('l', "cache-file").required().valueName("<file>").action( (x, c) =>
      c.copy(cacheFile = x) ).text("cacheFile is a required property where query last id is cached for later use")
    
    opt[String]('d', "warehouse-dir").required().valueName("<hdfs-path>").action( (x, c) =>
        c.copy(wareHouseDir = x) ).text("wareHouseDir is the HDFS warehouse directory where data will be saved")
    
    opt[String]('f', "file-format").action( (x, c) => 
      c.copy(fileFormat = x) ).text("fileFormat is the file format in which data is saved in the HDFS warehouse")
    
    opt[String]('c', "connect").required().valueName("<connection-string>").action( (x, c) => 
      c.copy(connectionString = x) ).text("JDBC Connection string. Currently supported JDBC drivers are MYSQL, ORACLE and MSSQL.")
    
    opt[String]('t', "table").required().valueName("<table-name>").action( (x, c) => 
      c.copy(table = x) ).text("Provide database table name to pull data from.")
    
    opt[String]('e', "es-host").valueName("<host>:<port>").action( (x, c) => 
      c.copy(esHost = x) ).text("Elasticsearch host and port together. Format: 197.253.1.252:9200")
    
    opt[String]('r', "es-resource").valueName("index:type").action( (x, c) => 
      c.copy(esResource = x) ).text("Elasticsearch resource i.e index and type. Format: index_name/type_name")
      
//    opt[String]('i', "check-column").valueName("<id column>").action( (x, c) => 
//      c.copy(checkColumn = x) ).text("Use either id column or timestamp column")
    
    help("help").abbr("h").text("Usage: type --help or -h to see the options")
  }
    
  def main(argv: Array[String]) {
    
    parser.parse(argv, CommandLineConfig()) match {
      case Some(config) => 
        
        val tableName = config.table
        val connectionString = config.connectionString
        val esHost = config.esHost
        val esResource = config.esResource
        val jdbcDriver = getConnectionDriver(connectionString.trim())
        println("print I have some stuff ", jdbcDriver, connectionString)
        val dataTable = sqlContext
                .read
                .format("jdbc")
                .options(
                    Map(
                        "url" -> connectionString.trim(),
                        "driver" -> jdbcDriver,
                        "dbTable" -> tableName.trim()
                        )
                 )
    
    
        val payloadDF = dataTable.load()
        
        val lastNumberFile = config.cacheFile
        val wareHouseDirectory = config.wareHouseDir
        val fileFormat = config.fileFormat
        val lastID = readLastFileNumber(lastNumberFile)
        val newRecordsDF = payloadDF.select("*").where(col("Id") > lastID)
        val max_id = newRecordsDF.select(max("Id"))
        val enrichedData = newRecordsDF.toJSON.map { x => parseJson(x) }.map(extractValues)
    
        writeToHDFS(newRecordsDF, wareHouseDirectory, AVRODATAFILE)
        
        if(! esHost.equals("*")) writeToES(enrichedData, esHost, esResource, Some("id"))
        
        max_id.rdd.foreach { x => 
           val id = x.get(0)
           if( id != null) writeLastFileNumber(lastNumberFile, id.toString())
        }
    
        val count = newRecordsDF.count()
    
        logger.info(s"Total of $count were retrieved from the table")
        logger.info(s"Shutting down Spark ETL process")
        
      case None => parser.help("help").abbr("h").text("Usage: type --help or -h to see the options")
    } 

  }
  
}