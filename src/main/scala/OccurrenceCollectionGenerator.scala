import java.util

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._

case class Occurrence(lat: String,
                      lng: String,
                      taxonPath: String,
                      eventDate: String,
                      id: String,
                      sourceDate: String,
                      source: String)

case class OccurrenceExt(lat: String,
                         lng: String,
                         taxonPath: String,
                         id: String,
                         pdate: Long,
                         psource: String,
                         start: Long,
                         end: Long)

case class OccurrenceSelector(taxonSelector: String = "", wktString: String = "", traitSelector: String = "")

object OccurrenceCollectionGenerator {

  def generateCollection(config: ChecklistConf) {
    val occurrenceFile = config.occurrenceFiles.head

    val conf = new SparkConf()
      .set("spark.cassandra.connection.host", "localhost")
      .setAppName("occ2collection")

    val selectorConfig = toOccurrenceSelector(config)

    val sc = new SparkContext(conf)
    sc.addSparkListener(new OccurrenceCollectionListener(selectorConfig))


    val occurrenceSelector = {
      if (config.firstSeenOnly) {
        OccurrenceCollectionBuilder.selectOccurrencesFirstSeenOnly _
      } else {
        OccurrenceCollectionBuilder.selectOccurrences _
      }
    }

    val sqlContext = SQLContextSingleton.getInstance(sc)

    val occurrenceCollection = load(occurrenceFile, sqlContext).transform[OccurrenceExt]({ ds =>
      occurrenceSelector(sqlContext, ds, OccurrenceSelectors.all(selectorConfig))
    })

    config.outputFormat.trim match {
      case "cassandra" =>
        saveCollectionToCassandra(sc, selectorConfig, occurrenceCollection)

      case _ =>
        println(s"unsupported output format [${config.outputFormat}]")
    }

  }

  def load(occurrenceFile: String, sqlContext: SQLContext): Dataset[Occurrence] = {
    val occurrences: DataFrame = sqlContext.read.format("parquet").load(occurrenceFile)
    val ds: Dataset[Occurrence] = OccurrenceCollectionBuilder.toOccurrenceDS(sqlContext, occurrences)
    ds
  }


  def toOccurrenceSelector(config: ChecklistConf): OccurrenceSelector = {
    val taxonSelector = config.taxonSelector
    val traitSelectors = config.traitSelector

    val wktString = config.geoSpatialSelector.trim
    val taxonSelectorString: String = taxonSelector.mkString("|")
    val traitSelectorString: String = traitSelectors.mkString("|")

    OccurrenceSelector(taxonSelectorString, wktString, traitSelectorString)
  }

  def saveCollectionToCassandra(sc: SparkContext, occurrenceSelector: OccurrenceSelector, occurrenceCollection: Dataset[OccurrenceExt]): Unit = {
    val sqlContext: SQLContext = SQLContextSingleton.getInstance(sc)
    import sqlContext.implicits._

    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.execute(CassandraUtil.checklistKeySpaceCreate)
      session.execute(CassandraUtil.occurrenceCollectionRegistryTableCreate)
      session.execute(CassandraUtil.occurrenceCollectionTableCreate)
      session.execute(CassandraUtil.occurrenceSearchTableCreate)
      session.execute(CassandraUtil.occurrenceFirstAddedSearchTableCreate)
    }

    val taxonSelectorString: String = occurrenceSelector.taxonSelector
    val wktString: String = occurrenceSelector.wktString
    val traitSelectorString: String = occurrenceSelector.traitSelector

    occurrenceCollection.map(item => {
      (taxonSelectorString, wktString, traitSelectorString,
        item.taxonPath, item.lat, item.lng,
        item.start, item.end,
        item.id,
        item.pdate, item.psource)
    }).rdd.saveToCassandra("effechecka", "occurrence_collection", CassandraUtil.occurrenceCollectionColumns)

    occurrenceCollection.map(item => {
      (item.psource, item.id, taxonSelectorString, wktString, traitSelectorString)
    }).rdd.saveToCassandra("effechecka", "occurrence_search", CassandraUtil.occurrenceSearchColumns)

    occurrenceCollection.map(item => {
      (item.psource, item.pdate, item.id)
    }).rdd.saveToCassandra("effechecka", "occurrence_first_added_search", CassandraUtil.occurrenceFirstAddedSearchColumns)

    sc.parallelize(Seq((taxonSelectorString, wktString, traitSelectorString, "ready", occurrenceCollection.count())))
      .saveToCassandra("effechecka", "occurrence_collection_registry", CassandraUtil.occurrenceCollectionRegistryColumns)
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
      opt[String]('f', "output-format") optional() valueName "<output format>" action { (x, c) =>
        c.copy(outputFormat = x)
      } text "output format"
      opt[String]('c', "<occurrence url>") required() action { (x, c) =>
        c.copy(occurrenceFiles = splitAndClean(x))
      } text "list of occurrence archive urls"
      opt[String]('t', "<traits url>") required() action { (x, c) =>
        c.copy(traitFiles = splitAndClean(x))
      } text "list of trait archive urls"

      arg[String]("<taxon selectors>") required() action { (x, c) =>
        c.copy(taxonSelector = splitAndClean(x))
      } text "pipe separated list of taxon names"
      arg[String]("<geospatial selector>") required() action { (x, c) =>
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

  def buildOccurrenceCollection(sc: SparkContext, df: Dataset[Occurrence], selector: (OccurrenceExt) => Boolean): Dataset[OccurrenceExt] = {
    selectOccurrences(SQLContextSingleton.getInstance(sc), df, selector)
  }

  def buildOccurrenceCollectionFirstSeenOnly(sc: SparkContext, ds: Dataset[Occurrence], selector: (OccurrenceExt) => Boolean): Dataset[OccurrenceExt] = {
    selectOccurrencesFirstSeenOnly(SQLContextSingleton.getInstance(sc), ds, selector)
  }


  def selectOccurrencesFirstSeenOnly(sqlContext: SQLContext, ds: Dataset[Occurrence], selector: (OccurrenceExt) => Boolean): Dataset[OccurrenceExt] = {
    val occurrences = selectOccurrences(sqlContext, ds, selector)
    firstSeenOccurrences(sqlContext, occurrences)
  }


  def selectOccurrences(sqlContext: SQLContext, ds: Dataset[Occurrence], selector: (OccurrenceExt) => Boolean): Dataset[OccurrenceExt] = {
    import sqlContext.implicits._

    val normalize: (Dataset[Occurrence] => Dataset[OccurrenceExt]) = {
      _.map(occ => {
        val startEnd = DateUtil.startEndDate(occ.eventDate)
        OccurrenceExt(
          lat = occ.lat, lng = occ.lng,
          taxonPath = occ.taxonPath,
          start = startEnd._1,
          end = startEnd._2,
          id = occ.id,
          pdate = DateUtil.basicDateToUnixTime(occ.sourceDate),
          psource = occ.source)
      })
    }

    ds
      .filter(x => DateUtil.nonEmpty(x.id))
      .filter(x => DateUtil.validDate(x.sourceDate))
      .filter(x => DateUtil.validDate(x.eventDate))
      .transform(normalize)
      .filter(selector)
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

  def firstSeenOccurrences(sqlContext: SQLContext, occurrences: Dataset[OccurrenceExt]): Dataset[OccurrenceExt] = {
    import sqlContext.implicits._

    occurrences
      .groupBy(_.id)
      .mapGroups((id, occIter) => occIter.reduce((occ, firstSeen) => {
        if (occ.pdate < firstSeen.pdate) {
          occ
        } else {
          firstSeen
        }
      }))
  }


}