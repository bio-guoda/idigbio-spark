import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.commons.logging.LogFactory
import org.apache.spark.storage.StorageLevel
import org.effechecka.selector.OccurrenceSelector

import scala.collection.JavaConversions

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

case class OccurrenceCassandra(lat: String,
                               lng: String,
                               taxon: String,
                               id: String,
                               added: Long,
                               source: String,
                               start: Long,
                               end: Long,
                               taxonselector: String,
                               wktstring: String,
                               traitselector: String)


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

object OccurrenceCollectionBuilder {
  private val latitudeTerm = "`http://rs.tdwg.org/dwc/terms/decimalLatitude`"
  private val longitudeTerm = "`http://rs.tdwg.org/dwc/terms/decimalLongitude`"
  val locationTerms = List(latitudeTerm, longitudeTerm)
  val taxonNameTerms = List("kingdom", "phylum", "class", "order", "family", "genus", "specificEpithet", "scientificName")
    .map(term => s"`http://rs.tdwg.org/dwc/terms/$term`")
  val eventDateTerm = "`http://rs.tdwg.org/dwc/terms/eventDate`"

  val occurrenceIdTerm = "`http://rs.tdwg.org/dwc/terms/occurrenceID`"
  val remainingTerms = List(eventDateTerm, occurrenceIdTerm, "`date`", "`source`")

  def availableTaxonTerms(implicit df: DataFrame): List[String] = {
    taxonNameTerms.intersect(availableTerms)
  }

  def availableTerms(implicit df: DataFrame): Seq[String] = {
    (locationTerms ::: taxonNameTerms ::: remainingTerms) intersect df.columns.map(_.mkString("`", "", "`"))
  }

  def mandatoryTermsAvailable(implicit df: DataFrame) = {
    (availableTerms.containsSlice(locationTerms)
      && availableTerms.containsSlice(remainingTerms)
      && availableTaxonTerms.nonEmpty)
  }

  def buildOccurrenceCollection(sc: SparkContext, df: Dataset[Occurrence], selectors: Seq[OccurrenceSelector]): Dataset[SelectedOccurrence] = {
    selectOccurrences(SQLContextSingleton.getInstance(sc), df, selectors)
  }

  def buildOccurrenceCollectionFirstSeenOnly(sc: SparkContext, ds: Dataset[Occurrence], selectors: Seq[OccurrenceSelector]): Dataset[SelectedOccurrence] = {
    selectOccurrencesFirstSeenOnly(SQLContextSingleton.getInstance(sc), ds, selectors)
  }


  def selectOccurrencesFirstSeenOnly(sqlContext: SQLContext, ds: Dataset[Occurrence], selectors: Seq[OccurrenceSelector]): Dataset[SelectedOccurrence] = {
    val occurrences = selectOccurrences(sqlContext, ds, selectors)
    firstSeenOccurrences(sqlContext, occurrences)
  }


  def selectOccurrences(sqlContext: SQLContext, ds: Dataset[Occurrence], selectors: Seq[OccurrenceSelector]): Dataset[SelectedOccurrence] = {
    import sqlContext.implicits._

    ds
      .filter(x => DateUtil.nonEmpty(x.id))
      .filter(x => DateUtil.validDate(x.sourceDate))
      .filter(x => DateUtil.validDate(x.eventDate))
      .flatMap(x => selectors.flatMap(selector => {
        if (OccurrenceSelectors.apply(selector)(x)) {
          Some(SelectedOccurrence(occ = x, selector = selector))
        } else {
          None
        }
      })
      )
  }

  def toOccurrenceDS(sqlContext: SQLContext, df: DataFrame): Dataset[Occurrence] = {
    import sqlContext.implicits._

    if (mandatoryTermsAvailable(df)) {
      val taxonPathTerm: String = "taxonPath"
      val withPath = df.select(availableTerms(df).map(col): _*)
        .withColumn(taxonPathTerm, concat_ws("|", availableTaxonTerms(df).map(col): _*))
      val occColumns = locationTerms ::: List(taxonPathTerm) ::: remainingTerms

      withPath.select(occColumns.map(col): _*)
        .withColumnRenamed("http://rs.tdwg.org/dwc/terms/decimalLatitude", "lat")
        .withColumnRenamed("http://rs.tdwg.org/dwc/terms/decimalLongitude", "lng")
        .withColumnRenamed("http://rs.tdwg.org/dwc/terms/eventDate", "eventDate")
        .withColumnRenamed("http://rs.tdwg.org/dwc/terms/occurrenceID", "id")
        .withColumnRenamed("date", "sourceDate")
        .as[Occurrence]
    } else {
      sqlContext.emptyDataFrame.as[Occurrence]
    }

  }

  def firstSeenOccurrences(sqlContext: SQLContext, occurrences: Dataset[SelectedOccurrence]): Dataset[SelectedOccurrence] = {
    import sqlContext.implicits._

    occurrences.rdd
      .map(selOcc => ((selOcc.occ.id, selOcc.selector), selOcc))
      .reduceByKey((agg, occ) => occ.copy(occ = DateUtil.selectFirstPublished(agg.occ, occ.occ)))
      .map(_._2)
      .toDS()
  }

}