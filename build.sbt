name := "iDigBio-LD"

version := "1.5.8"

scalaVersion := "2.11.11"

val sparkV: String = "2.1.1"

libraryDependencies ++= Seq(
  "net.sf.opencsv" % "opencsv" % "2.3",
  "org.apache.commons" % "commons-csv" % "1.1",
  "com.github.scopt" %% "scopt" % "3.3.0",
  "io.spray" %%  "spray-json" % "1.3.2",
  "org.globalnames" %% "gnparser" % "0.3.3",
  "org.locationtech.spatial4j" % "spatial4j" % "0.6",
  "com.vividsolutions" % "jts-core" % "1.14.0",
  "org.apache.spark" %% "spark-sql" % sparkV % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.2",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "com.holdenkarau" %% "spark-testing-base" % s"2.1.0_0.6.0" % "test"
)

test in assembly := {}

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")

resolvers += Resolver.sonatypeRepo("public")
