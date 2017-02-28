package org.custom.etl.models

class Model {
  
}
import java.io.File

case class CommandLineConfig(cacheFile: String = "/tmp/lastidcache.txt", 
                            wareHouseDir: String = "user/hive/warehouse/spark_etl", 
                            fileFormat: String = "avro",
                            connectionString: String = "",
                            table: String = "",
                            esHost: String = "*",
                            esResource: String = "test/test_type")
                            
case class ConnectionObject(host: String, username: String, password: String, port: Option[String] = None, service: Option[String] = None)
