package org.custom.etl.utils

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.custom.etl.models.ConnectionObject
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.elasticsearch.spark.rdd.EsSpark
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import org.apache.spark.rdd.RDD
import scala.util.parsing.json.JSON._
import scala.util.parsing.json._


case class ConnectionProfile(connectrionString: String, jdbcDriver: String, tableName: Option[String] = None)

object Services extends LogHelper {
  
  /*
   * This method has been deprecated
  def getParam(key: String) = {
    val config: Config = ConfigFactory.load()
    config.getString(key)
  }
  * 
  */
  
  def writeLastFileNumber(filePath: String, id: String) {
    import java.io._
    val file = new File(filePath)
    val bw = new BufferedWriter(new FileWriter(file))
    logger.info(s"Caching last id: $id for later use in $file")
    bw.write(id)
    bw.close()
  }
  
  def readLastFileNumber(filePath: String): Int = {
    logger.info(s"Reading last id from $filePath")
    import scala.io.Source
    val lastId = Source.fromFile(filePath).getLines.mkString
    logger.info(s"Last query id: $lastId")
    lastId.trim().toInt
  }
  
  def getConnectionDriver(connectionString: String) = {
    val driver = 
      if(connectionString.contains("sqlserver")) "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      else if(connectionString.contains("mysql")) "com.mysql.jdbc.Driver"
      else if(connectionString.contains("oracle")) "oracle.jdbc.driver.OracleDriver"
      else "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    driver
  }
  
  def buildConnectionProfile (connectionObject: ConnectionObject, dbType: DBTYPE): ConnectionProfile = {
    val (host, username, password) = (connectionObject.host, connectionObject.username, connectionObject.password) 
    val port = if(connectionObject.port.isDefined) connectionObject.port.get else ""
    val service = if(connectionObject.service.isDefined) connectionObject.service.get else ""
    
    val connectionString = dbType match {
      case MSSQL => logger.info(s"Connecting to jdbc:sqlserver://$host;username=$username. Building connection profile for $dbType"); 
        ConnectionProfile(s"jdbc:sqlserver://$host;username=$username;password=$password", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      case MYSQL => logger.info(s"Connecting to jdbc:sqlserver://$host;username=$username. Building connection profile for $dbType"); 
        ConnectionProfile(s"jdbc:mysql://$host:$port?user=$username&password=$password", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      case ORACLEDB => logger.info(s"Connecting to jdbc:sqlserver://$host;username=$username. Building connection profile for $dbType");
        ConnectionProfile(s"jdbc:oracle:thin:$username/$password@$host:$port:$service", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    }
    connectionString
  }
  
  def writeToHDFS(dataFrame: DataFrame, hdfsPath: String, fileFormat: FILEFORMAT) {
    fileFormat match {
      case AVRODATAFILE => logger.info(s"Writing data to HDFS with $fileFormat format. HDFS File path $hdfsPath");
        dataFrame.write.format("com.databricks.spark.avro").mode(SaveMode.Append).save(hdfsPath)
      case PARQUETDATAFILE => logger.info(s"Writing data to HDFS with $fileFormat format. HDFS File path $hdfsPath");
        dataFrame.write.mode(SaveMode.Append).parquet(hdfsPath)
    }
  }
  
  def writeToES(dataRDD: RDD[Map[String, Any]], host: String, resource: String, id: Option[String] = None) {
    logger.info(s"Writing data to Elasticsearch. ES Resources: index: $resource")
    val config = Map("es.mapping.id" -> "id")
    val cfg = Map("es.nodes" -> host, "es.nodes.discovery" -> "false")
    EsSpark.saveToEs(dataRDD, resource, cfg)
  }
  
  def parseJson(jsonString: String) = {
    val json = parseFull(jsonString)
    json.get.asInstanceOf[Map[String, Any]]
  }
  
}

trait DBTYPE
case object MSSQL extends DBTYPE
case object MYSQL extends DBTYPE
case object ORACLEDB extends DBTYPE
case object POSTGRESQL extends DBTYPE
case object REDSHIFT extends DBTYPE

trait FILEFORMAT
case object AVRODATAFILE extends FILEFORMAT
case object PARQUETDATAFILE extends FILEFORMAT