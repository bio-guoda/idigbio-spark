name := "iDigBio-LD"

version := "1.5.9"

scalaVersion := "2.11.12"

val sparkV: String = "2.2.3"

libraryDependencies ++= Seq(
  "net.sf.opencsv" % "opencsv" % "2.3",
  "org.apache.commons" % "commons-csv" % "1.1",
  "com.github.scopt" %% "scopt" % "3.3.0",
  "io.spray" %%  "spray-json" % "1.3.2",
  "org.globalnames" %% "gnparser" % "0.3.3" excludeAll( ExclusionRule(organization = "com.fasterxml.jackson.core") ),
  "org.effechecka" %% "effechecka-selector" % "0.0.3",
  "org.apache.spark" %% "spark-sql" % sparkV % "provided",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "com.databricks" %% "spark-avro" % "4.0.0",
  "com.holdenkarau" %% "spark-testing-base" % s"2.2.0_0.10.0" % "test"

)

test in assembly := {}

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")

resolvers += Resolver.sonatypeRepo("public")
resolvers += "effechecka-releases" at "https://s3.amazonaws.com/effechecka/releases"

