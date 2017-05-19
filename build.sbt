name := "iDigBio-LD"

version := "1.5.7"

scalaVersion := "2.11.8"

val sparkV: String = "2.0.1"

libraryDependencies ++= Seq(
  "net.sf.opencsv" % "opencsv" % "2.3",
  "org.apache.commons" % "commons-csv" % "1.1",
  "com.github.scopt" %% "scopt" % "3.3.0",
  "io.spray" %%  "spray-json" % "1.3.2",
  "org.globalnames" %% "gnparser" % "0.3.3",
  "org.locationtech.spatial4j" % "spatial4j" % "0.6",
  "com.vividsolutions" % "jts-core" % "1.14.0",
  "org.apache.spark" %% "spark-sql" % sparkV % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-8" % sparkV exclude ("org.spark-project.spark", "unused"),
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0-M3",
  "org.scalatest" %% "scalatest" % "2.2.5" % "test",
  "com.holdenkarau" %% "spark-testing-base" % s"${sparkV}_0.4.7" % "test"
)

test in assembly := {}

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

resolvers += Resolver.sonatypeRepo("public")
