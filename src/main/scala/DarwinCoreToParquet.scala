import java.io.File
import java.net.{URI, URL}
import java.nio.file.attribute.UserPrincipal
import java.nio.file.{Path, Files, Paths}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.datastax.spark.connector._
import scopt._
import DwC.Meta

import scala.collection.JavaConversions._

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
      meta.fileURIs map { fileLocation =>
        println(s"attempting to load [$fileLocation]...")
        val df = sqlCtx.read.format("csv").
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
        val ctx: SparkContext = new SparkContext(conf)
        sqlContext = new SQLContext(ctx)
        for (archive <- config.archives) {
          println(s"attempting to process dwc meta [$archive]")
        }

        val metas = parseMeta(config.archives)
        for ((sourceLocation, df) <- metaToDF(sqlCtx = sqlContext, metas = metas)) {
          df.write.format("parquet").save(parquetPathString(sourceLocation))
        }

        setParquetOwnerToSourceOwner(metas)
        SparkUtil.stopAndExit(sc)
      }
      case None =>
      // arguments are bad, error message will have been displayed
    }

  }

  def setParquetOwnerToSourceOwner(metas: Seq[Meta]): Any = {
    val existingSourceParquetPathPairs = metas
      .flatMap(_.fileURIs)
      .map((fileLocation: String) => {
        val sourceURI = new URI(fileLocation)
        val parquetURI = new URI(parquetPathString(fileLocation))
        val (source, parquet) = (Paths.get(sourceURI), Paths.get(parquetURI))
        println("attempting to transfer ownership of [" + parquet + "] to owner of [" + source + "]")
        (source, parquet)
      })
      .filter { case (source: Path, parquet: Path) => Files.exists(source) && Files.exists(parquet) }

    existingSourceParquetPathPairs.foreach {
      case (sourcePath, parquetPath) => {
        transferToOwnerOf(parquetPath, sourcePath)
        parquetPath.toFile.listFiles().foreach((file: File) => transferToOwnerOf(file.toPath, sourcePath))
      }
    }
  }

  def transferToOwnerOf(parquetPath: Path, sourcePath: Path): Path = {
    val owner: UserPrincipal = Files.getOwner(sourcePath)
    println(s"attempting to transfer ownership for [$parquetPath] to owner [${owner.getName}] of [$sourcePath]")
    Files.setOwner(parquetPath, owner)
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
