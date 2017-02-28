// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }


// Project name
name := """CUSTOM_ETL_SPARK2ES"""

// Don't forget to set the version
version := "0.1.0-SNAPSHOT"

// All Spark Packages need a license
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

// scala version to be used
scalaVersion := "2.10.6"
// force scalaVersion
//ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

// spark version to be used
val sparkVersion = "1.6.0"

// Needed as SBT's classloader doesn't work well with Spark
fork := true

// BUG: unfortunately, it's not supported right now
fork in console := true

// Java version
javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

// add a JVM option to use when forking a JVM for 'run'
javaOptions ++= Seq("-Xmx2G")

// append -deprecation to the options passed to the Scala compiler
scalacOptions ++= Seq("-deprecation", "-unchecked")

// Use local repositories by default
resolvers ++= Seq(
  Resolver.defaultLocal,
  Resolver.mavenLocal,
  // make sure default maven local repository is added... Resolver.mavenLocal has bugs.
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
  // For Typesafe goodies, if not available through maven
  // "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  // For Spark development versions, if you don't want to build spark yourself
  "Apache Staging" at "https://repository.apache.org/content/repositories/staging/"
  )


val sparkDependencyScope = "provided"

// spark modules (should be included by spark-sql, just an example)
libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark"  %% "spark-sql" % sparkVersion % "provided",
  "org.elasticsearch" %% "elasticsearch-spark" % "2.2.0-m1" % "compile",
  "com.databricks" 	  %% "spark-avro" 			   % "2.0.1",
  "org.apache.avro"   % "avro" % "1.7.7",
  "org.apache.avro"   % "avro-maven-plugin" % "1.7.7",
  "org.apache.avro"   % "avro-mapred" % "1.7.7" classifier "hadoop2",
  "joda-time" 		  % "joda-time"	 		% "2.9.2",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.4",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.7.4",
  "com.oracle" % "ojdbc6" % "11.2.0.3",
  "mysql" % "mysql-connector-java" % "5.1.6",
  "com.microsoft.sqlserver" % "sqljdbc4" % "4.0"
)

libraryDependencies += "com.github.scopt" %% "scopt" % "3.5.0"

/// Compiler plugins

// linter: static analysis for scala
resolvers += "Linter Repository" at "https://hairyfotr.github.io/linteRepo/releases"
resolvers += "Oracle repo" at "https://code.lds.org/nexus/content/groups/main-repo"
resolvers += "Alfred Recso" at "https://artifacts.alfresco.com/nexus/content/groups/public"
resolvers += Resolver.sonatypeRepo("public")



// If you want to use yarn-client for spark cluster mode, override the environment variable
// SPARK_MODE=yarn-client <cmd>
val sparkMode = sys.env.getOrElse("SPARK_MODE", "local[2]")


initialCommands in console :=
  s"""
    |import org.apache.spark.SparkConf
    |import org.apache.spark.SparkContext
    |import org.apache.spark.SparkContext._
    |
    |@transient val sc = new SparkContext(
    |  new SparkConf()
    |    .setMaster("$sparkMode")
    |    .setAppName("Console test"))
    |implicit def sparkContext = sc
    |import sc._
    |
    |@transient val sqlc = new org.apache.spark.sql.SQLContext(sc)
    |implicit def sqlContext = sqlc
    |import sqlc._
    |
    |def time[T](f: => T): T = {
    |  import System.{currentTimeMillis => now}
    |  val start = now
    |  try { f } finally { println("Elapsed: " + (now - start)/1000.0 + " s") }
    |}
    |
    |""".stripMargin

cleanupCommands in console :=
  s"""
     |sc.stop()
   """.stripMargin


autoAPIMappings := true

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
