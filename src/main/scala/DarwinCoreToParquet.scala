import java.net.URI

import bio.guoda.DwC
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import bio.guoda.DwC.Meta


trait DwCHandler {
  def toDF2(sqlCtx: SQLContext, metas: Seq[String]): Seq[(String, DataFrame)] = {
    metaToDF(sqlCtx: SQLContext, parseMeta(metas.map((_, None))))
  }

  def parseMeta(metaLocators: Seq[(String, Option[String])]): Seq[Meta]

  def metaToDF(sqlCtx: SQLContext, metas: Seq[Meta]): Seq[(String, DataFrame)]
}

trait DwCSparkHandler extends DwCHandler {

  def parseMeta(metaLocators: Seq[(String, Option[String])]): Seq[Meta] = {
    val metaURIs: Seq[(URI, Option[String])] = metaLocators map { meta => (URI.create(meta._1), meta._2) }

    metaURIs flatMap { case(metaURL: URI, xmlString: Option[String]) => {
      DwC.readMeta(metaURL, xmlString)
    }
    }
  }

  def metaToDF(sqlCtx: SQLContext, metas: Seq[Meta]): Seq[(String, DataFrame)] = {
    implicit val ctx: SQLContext = sqlCtx
    DwC.asDF(metas)
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
        try {
          sqlContext = new SQLContext(ctx)
          for (archive <- config.archives) {
          }

          val metaURIsAndTheirXmlStrings = config.archives map {
            archive => {
              Console.err.print(s"[$archive] reading...")
              val xmlString = Some(sqlContext.read.textFile(archive).collect.mkString)
              Console.err.println(s" done.")
              (archive, xmlString)
            }
          }
          val metas = parseMeta(metaURIsAndTheirXmlStrings)
          for ((sourceLocation, df) <- metaToDF(sqlCtx = sqlContext, metas = metas)) {
            val parquetFile = parquetPathString(sourceLocation)
            Console.err.print(s"[$parquetFile] saving...")
            df.write.format("parquet").save(parquetFile)
            println(parquetFile)
            Console.err.println(s" done.")
          }
        } catch {
          case e: Throwable => {
            Console.err.println()
            Console.err.println("failed due to: [" + e.getMessage + "]")
            SparkUtil.stopAndExit(sc, 1)
          }
        } finally {
          SparkUtil.stopAndExit(sc, 0)
        }
      }
      case None =>
        // arguments are bad, error message will have been displayed
        SparkUtil.stopAndExit(sc, 1)
    }

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
