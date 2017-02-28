# Custom ETL Spark2ES

## Highlights

- Load Data from MSSQL Database into Elasticsearch
- Save the data into HDFS with either Avro or Parquet
- Can be extended for other custom uses

## Libraries included

- Spark 1.6.0
- Scala 2.10.6
- Elasticsearch-Spark 2.2.0-m1
- Microsoft SQL Server JDBC 4.0
- Scopt 3.5.0

## Usage

You can run on local mode or in your spark cluster. Just ensure you have your configurations correctly. However, we strongly advise you run in cluster mode for better performance.

The compiled jar file also accepts command line arguments

Usage: scopt [options]

  `-l, --cache-file <file>  cacheFile is a required property where query last id is cached for later use`

  `-d, --warehouse-dir <hdfs-path> wareHouseDir is the HDFS warehouse directory where data will be saved`

  `-f, --file-format <value> fileFormat is the file format in which data is saved in the HDFS warehouse`

  `-c, --connect <connection-string> JDBC Connection string. Currently supported JDBC drivers are MYSQL, ORACLE and MSSQL.`

  `-t, --table <table-name> Provide database table name to pull data from.`

  `-e, --es-host <host>:<port> Elasticsearch host and port together. Format: 197.253.1.252:9200`

  `-r, --es-resource index:type Elasticsearch resource i.e index and type. Format: index_name/type_name`

  `-h, --help               Usage: type --help or -h to see the options`


**To run**

spark-submit <fat-jar-file.jar> [options]

## Issues
For additional features, log issues and we will attend to them

### Contribution

We welcome pull requests as we hope that this can become a generic ETL tool built on Spark

## Author

- Olalekan Elesin (elesin.olalekan@gmail.com) and Simeon Babatunde (babatunde.simeon@gmail.com)
