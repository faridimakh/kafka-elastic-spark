name := "kafka-elastic-spark"

version := "0.1"
val spark_version = "2.3.1"
scalaVersion := "2.11.8"

val ConfluentVersion: String = "5.5.1"

resolvers += "Confluent".at("https://packages.confluent.io/maven/")

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.8.0"
libraryDependencies += "io.confluent"     % "kafka-schema-registry-client" % ConfluentVersion
libraryDependencies += "io.confluent"     % "kafka-avro-serializer"        % ConfluentVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value
)

dependencyOverrides ++= {
  Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7.1",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7",
    "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"
  )
}
