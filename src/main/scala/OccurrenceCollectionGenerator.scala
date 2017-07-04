import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.{Dataset, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.effechecka.selector.OccurrenceSelector

case class Occurrence(lat: String,
                      lng: String,
                      taxonPath: String,
                      eventDate: String,
                      id: String,
                      sourceDate: String,
                      source: String)

case class SelectedOccurrence(occ: Occurrence, selector: OccurrenceSelector)

case class MonitoredOccurrence(source: String, id: String, taxonselector: String, wktstring: String, traitselector: String)

case class FirstOccurrence(source: String, added: Long, id: String)



object OccurrenceCollectionGenerator {

  def generateCollection(config: ChecklistConf) {

    val conf = new SparkConf()
      .set("spark.debug.maxToStringFields", "250") // see https://issues.apache.org/jira/browse/SPARK-15794
      .set("spark.cassandra.output.batch.grouping.key", "None")
      .set("spark.cassandra.output.batch.size.rows", "10")
      .set("spark.cassandra.output.batch.size.bytes", "2048")
      .set("spark.cassandra.output.throughput_mb_per_sec", "5") // see https://www.instaclustr.com/blog/2016/03/31/cassandra-connector-for-spark-5-tips-for-success/
      .setAppName("occ2collection")

    val sc = SparkUtil.start(conf)
    try {
      val occurrenceCollector = config.outputFormat.trim match {
        case "cassandra" =>
          new OccurrenceCollectorCassandra()
        case "hdfs" =>
          new OccurrenceCollectorHDFS()

        case _ =>
          class OccurrenceCollectorNotSupported extends OccurrenceCollector {
            def write(config: ChecklistConf, sc: SparkContext): Unit = SparkUtil.logInfo(s"unsupported output format [${config.outputFormat}]")
          }
          new OccurrenceCollectorNotSupported()
      }
      occurrenceCollector.write(config, sc)
    } catch {
      case e: Throwable => LogFactory.getLog(getClass).error("failed to generate occurrence collection", e)
    } finally {
      SparkUtil.stopAndExit(sc)
    }
  }

  def load(occurrenceFile: String, sqlContext: SQLContext): Dataset[Occurrence] = {
    val occurrences = ParquetUtil.readParquet(path = occurrenceFile, sqlContext = sqlContext)
    OccurrenceCollectionBuilder.toOccurrenceDS(sqlContext, occurrences)
  }


  def main(args: Array[String]) {
    config(args) match {
      case Some(c) =>
        generateCollection(c)
      case _ =>
    }
  }

  def config(args: Array[String]): Option[ChecklistConf] = {

    def splitAndClean(arg: String): Seq[String] = {
      arg.trim.split( """[\|,]""").toSeq.filter(_.nonEmpty)
    }

    val parser = new scopt.OptionParser[ChecklistConf]("occ2collection") {
      head("occ2collection", "0.x")
      opt[Boolean]('s', "first-seen") optional() valueName "<first seen occurrences only>" action { (x, c) =>
        c.copy(firstSeenOnly = x)
      } text "include only first seen occurrences, removing duplicates"
      opt[Boolean]('a', "apply-all-selectors") optional() valueName "<apply all selectors>" action { (x, c) =>
        c.copy(applyAllSelectors = x)
      } text "generate occurrence collections for all selectors"
      opt[String]('f', "output-format") optional() valueName "<output format>" action { (x, c) =>
        c.copy(outputFormat = x)
      } text "output format"
      opt[String]('o', "output-path") optional() valueName "<output path>" action { (x, c) =>
        c.copy(outputPath = x)
      } text "output path"
      opt[String]('c', "<occurrence url>") required() action { (x, c) =>
        c.copy(occurrenceFiles = splitAndClean(x))
      } text "list of occurrence archive urls"
      opt[String]('t', "<traits url>") required() action { (x, c) =>
        c.copy(traitFiles = splitAndClean(x))
      } text "list of trait archive urls"

      arg[String]("<taxon selectors>") optional() action { (x, c) =>
        c.copy(taxonSelector = splitAndClean(x))
      } text "pipe separated list of taxon names"
      arg[String]("<geospatial selector>") optional() action { (x, c) =>
        c.copy(geoSpatialSelector = x.trim)
      } text "WKT string specifying an geospatial area of interest"
      arg[String]("trait selectors") optional() action { (x, c) =>
        c.copy(traitSelector = splitAndClean(x))
      } text "pipe separated list of trait criteria"
    }

    parser.parse(args, ChecklistConf())
  }
}

