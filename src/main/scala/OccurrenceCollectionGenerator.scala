import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, Dataset, DataFrame, SQLContext}
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

case class OccurrenceSelector(taxonSelector: String = "", wktString: String = "", traitSelector: String = "")

object OccurrenceCollectionGenerator {

  def generateCollection(config: ChecklistConf) {
    val occurrenceFile = config.occurrenceFiles.head

    val conf = new SparkConf()
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.cassandra.output.batch.grouping.key", "None")
      .set("spark.cassandra.output.batch.size.rows", "10")
      .set("spark.cassandra.output.batch.size.bytes", "2048")
      .setAppName("occ2collection")

    val sc = new SparkContext(conf)
    val occurrenceSelectors = occurrenceSelectorsFor(config, sc)

    val selectors: Broadcast[Seq[OccurrenceSelector]] = sc.broadcast(occurrenceSelectors)

    val applySelectors = {
      if (config.firstSeenOnly) {
        OccurrenceCollectionBuilder.selectOccurrencesFirstSeenOnly _
      } else {
        OccurrenceCollectionBuilder.selectOccurrences _
      }
    }

    val sqlContext = SQLContextSingleton.getInstance(sc)
    import sqlContext.implicits._
    val occurrenceCollection = load(occurrenceFile, sqlContext).transform[SelectedOccurrence]({ ds =>
      applySelectors(sqlContext, ds, selectors.value)
    })

    occurrenceCollection.cache()

    config.outputFormat.trim match {
      case "cassandra" =>
        saveCollectionToCassandra(sqlContext, selectors.value, occurrenceCollection)
      case _ =>
        println(s"unsupported output format [${config.outputFormat}]")
    }

  }

  def occurrenceSelectorsFor(config: ChecklistConf, sc: SparkContext): Seq[OccurrenceSelector] = {
    if (config.applyAllSelectors) {
      val cc = new CassandraSQLContext(sc)
      val selectorsDF = cc.sql("select taxonselector, wktstring, traitselector from effechecka.occurrence_collection_registry")
      selectorsDF.map(row =>
        OccurrenceSelector(row.getString(0), row.getString(1), row.getString(2))
      ).collect().toSeq
    } else {
      val selectorConfig = toOccurrenceSelector(config)
      sc.addSparkListener(new OccurrenceCollectionListener(selectorConfig))
      Seq(selectorConfig)
    }
  }

  def load(occurrenceFile: String, sqlContext: SQLContext): Dataset[Occurrence] = {
    val occurrences: DataFrame = sqlContext.read.format("parquet").load(occurrenceFile)
    OccurrenceCollectionBuilder.toOccurrenceDS(sqlContext, occurrences)
  }


  def toOccurrenceSelector(config: ChecklistConf): OccurrenceSelector = {
    val taxonSelector = config.taxonSelector
    val traitSelectors = config.traitSelector

    val wktString = config.geoSpatialSelector.trim
    val taxonSelectorString: String = taxonSelector.mkString("|")
    val traitSelectorString: String = traitSelectors.mkString("|")

    OccurrenceSelector(taxonSelectorString, wktString, traitSelectorString)
  }

  def saveCollectionToCassandra(sqlContext: SQLContext, occurrenceSelectors: Seq[OccurrenceSelector], occurrenceCollection: Dataset[SelectedOccurrence]): Unit = {
    import sqlContext.implicits._

    CassandraConnector(sqlContext.sparkContext.getConf).withSessionDo { session =>
      session.execute(CassandraUtil.checklistKeySpaceCreate)
      session.execute(CassandraUtil.occurrenceCollectionRegistryTableCreate)
      session.execute(CassandraUtil.occurrenceCollectionTableCreate)
      session.execute(CassandraUtil.occurrenceSearchTableCreate)
      session.execute(CassandraUtil.occurrenceFirstAddedSearchTableCreate)
    }

    val normalize: (Dataset[SelectedOccurrence] => Dataset[OccurrenceCassandra]) = {
      _.map(selectedOcc => {
        val occ = selectedOcc.occ
        val startEnd = DateUtil.startEndDate(occ.eventDate)
        OccurrenceCassandra(
          taxonselector = selectedOcc.selector.taxonSelector,
          wktstring = selectedOcc.selector.wktString,
          traitselector = selectedOcc.selector.traitSelector,
          lat = occ.lat, lng = occ.lng,
          taxon = occ.taxonPath,
          start = startEnd._1,
          end = startEnd._2,
          id = occ.id,
          added = DateUtil.basicDateToUnixTime(occ.sourceDate),
          source = occ.source)
      })
    }

    val occurrencesForCassandra = occurrenceCollection
      .transform(normalize)

    occurrencesForCassandra.toDF()
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "occurrence_collection", "keyspace" -> "effechecka"))
      .mode(SaveMode.Append)
      .save()


    occurrencesForCassandra.map(item => {
      MonitoredOccurrence(source = item.source, id = item.id, taxonselector = item.taxonselector, wktstring = item.wktstring, traitselector = item.traitselector)
    }).toDF()
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "occurrence_search", "keyspace" -> "effechecka"))
      .mode(SaveMode.Append)
      .save()


    occurrencesForCassandra.map(item => {
      FirstOccurrence(source = item.source, added = item.added, id = item.id)
    }).toDF()
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "occurrence_first_added_search", "keyspace" -> "effechecka"))
      .mode(SaveMode.Append)
      .save()

    occurrenceSelectors.foreach(selector => {
      val countBySelector: Long = occurrenceCollection.filter(_.selector == selector).count()
      sqlContext.sparkContext.parallelize(Seq((selector.taxonSelector, selector.wktString, selector.traitSelector, "ready", countBySelector)))
        .saveToCassandra("effechecka", "occurrence_collection_registry", CassandraUtil.occurrenceCollectionRegistryColumns)
    }
    )

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

    occurrences
      .groupBy($"occ.id", $"selector.taxonSelector", $"selector.wktString", $"selector.traitSelector")
      .reduce((selected: SelectedOccurrence, firstSelected: SelectedOccurrence) => {
        SelectedOccurrence(DateUtil.selectFirstPublished(selected.occ, firstSelected.occ), firstSelected.selector)
      }).map(_._2)
  }

}