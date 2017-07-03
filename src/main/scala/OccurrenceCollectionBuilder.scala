import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import org.effechecka.selector.OccurrenceSelector

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
