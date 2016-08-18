import java.net.URL
import java.nio.file.attribute.UserPrincipal
import java.nio.file.{Path, Files, Paths}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.datastax.spark.connector._
import scopt._
import DwC.Meta

import scala.collection.JavaConversions

trait DwCHandler {
  def toDF2(sqlCtx: SQLContext, metas: Seq[String]): Seq[(String, DataFrame)] = {
    metaToDF(sqlCtx: SQLContext, parseMeta(metas))
  }

  def parseMeta(metaLocators: Seq[String]): Seq[Meta]

  def metaToDF(sqlCtx: SQLContext, metas: Seq[Meta]): Seq[(String, DataFrame)]
}

trait DwCSparkHandler extends DwCHandler {

  def parseMeta(metaLocators: Seq[String]): Seq[Meta] = {
    val metaURLs: Seq[URL] = metaLocators map { meta => new URL(meta) }
    metaURLs flatMap { metaURL: URL => DwC.readMeta(metaURL) }
  }

  def metaToDF(sqlCtx: SQLContext, metas: Seq[Meta]): Seq[(String, DataFrame)] = {
    val metaDFTuples = metas map { meta: Meta =>
      val schema = StructType(meta.coreTerms map {
        StructField(_, StringType)
      })
      meta.fileLocations map { fileLocation =>
        println(s"attempting to load [$fileLocation]...")
        val df = sqlCtx.read.format("com.databricks.spark.csv").
          option("delimiter", meta.delimiter).
          option("quote", meta.quote).
          schema(schema).
          load(fileLocation.toString)
        val exceptHeaders = df.except(df.limit(meta.skipHeaderLines))
        (fileLocation, exceptHeaders)
      }
    }
    metaDFTuples.flatten
  }
}

object DarwinCoreToParquet extends DwCSparkHandler {
  implicit var sc: SparkContext = _
  implicit var sqlContext: SQLContext = _

  case class Config(archives: Seq[String] = Seq())

  def parquetPathString(sourceLocation: String): String = {
    sourceLocation + ".parquet"
  }


  def main(args: Array[String]) {

    config(args) match {
      case Some(config) => {
        val conf = new SparkConf()
          .setAppName("dwc2parquet")
        sqlContext = new SQLContext(new SparkContext(conf))
        for (archive <- config.archives) {
          println(s"attempting to process dwc meta [$archive]")
        }

        val metas = parseMeta(config.archives)
        for ((sourceLocation, df) <- metaToDF(sqlCtx = sqlContext, metas = metas)) {
          df.write.format("parquet").save(parquetPathString(sourceLocation))
        }

        setParquetOwnerToSourceOwner(metas)
      }
      case None =>
      // arguments are bad, error message will have been displayed
    }

  }

  def setParquetOwnerToSourceOwner(metas: Seq[Meta]): Any = {
    val existingSourceParquetPathPairs = metas
      .flatMap(_.fileLocations)
      .map((fileLocation: String) => {
        val (source, parquet) = (Paths.get(fileLocation), Paths.get(parquetPathString(fileLocation)))
        println("attempting to transfer ownership of [" + parquet + "] to owner of [" + source + "]")
        (source, parquet)
      })
      .filter { case (source: Path, parquet: Path) => Files.exists(source) && Files.exists(parquet) }

    existingSourceParquetPathPairs.foreach {
      case (sourcePath, parquetPath) => {
        val sourceOwner = Files.getOwner(sourcePath)
        setOwner(parquetPath, sourceOwner)
        JavaConversions.asScalaIterator(parquetPath.iterator()).foreach(setOwner(_, sourceOwner))
      }
    }
  }

  def setOwner(parquetPath: Path, sourceOwner: UserPrincipal): Path = {
    println("attempting to transfer ownership for [" + parquetPath + "] to [" + sourceOwner.getName + "]")
    Files.setOwner(parquetPath, sourceOwner)
  }

  def config(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("dwcToParquet") {
      head("dwcToParquet", "0.x")
      arg[String]("<dwc meta.xml url> ...") unbounded() required() action { (x, c) =>
        c.copy(archives = c.archives :+ x)
      } text "list of darwin core archives"
    }

    parser.parse(args, Config())
  }

}
