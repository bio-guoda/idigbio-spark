import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SQLContext, SaveMode}
import org.effechecka.selector.{OccurrenceSelector, UuidUtils}
import org.joda.time.format.ISODateTimeFormat

case class OccurrenceHDFS(lat: String,
                          lng: String,
                          taxon: String,
                          id: String,
                          added: Long,
                          source: String,
                          start: Long,
                          end: Long,
                          u0: String,
                          u1: String,
                          u2: String,
                          uuid: String,
                          y: String,
                          m: String,
                          d: String)

case class SourceMonitoredOccurrenceHDFS(source: String, id: String, uuid: String, y: String, m: String, d: String)

case class MonitorOfOccurrenceHDFS(uuid: String, source: String, u0: String, u1: String, u2: String, monitorUUID: String)

case class OccurrenceSummaryHDFS(uuid: String, u0: String, u1: String, u2: String,
                                 itemCount: Long,
                                 status: String = "ready",
                                 taxonSelector: String = "",
                                 wktString: String = "",
                                 traitSelector: String = "",
                                 lastModified: Long = new Date().getTime
                                )


class OccurrenceCollectorHDFS extends OccurrenceCollector {

  val occurrenceSummaryPath = "/occurrence-summary"

  def write(config: ChecklistConf, sc: SparkContext): Unit = {
    val sqlContext = SQLContextSingleton.getInstance(sc)
    val (saveMode, occurrenceSelectors) = if (config.applyAllSelectors) {
      (SaveMode.Overwrite, allSelectorsFor(sqlContext, config.outputPath))
    } else {
      (SaveMode.Append, Seq(OccurrenceSelectors.toOccurrenceSelector(config)))
    }

    val selectors: Broadcast[Seq[OccurrenceSelector]] = sc.broadcast(occurrenceSelectors)
    val occurrenceCollection = occurrenceCollectionsFor(config, sqlContext, selectors)
    writeAll(outputPath = config.outputPath, saveMode = saveMode, selectors = occurrenceSelectors, occurrences = occurrenceCollection)

  }

  def writeAll(selectors: Seq[OccurrenceSelector], occurrences: Dataset[SelectedOccurrence], outputPath: String, saveMode: SaveMode) = {
    writeOccurrences(occurrences, outputPath, saveMode)
    writeSummary(selectors, occurrences, outputPath)
  }

  def allSelectorsFor(sqlContext: SQLContext, outputPath: String): Seq[OccurrenceSelector] = {
    import sqlContext.implicits._
    sqlContext.read.parquet(s"$outputPath/$occurrenceSummaryPath")
      .as[OccurrenceSummaryHDFS]
      .rdd.map(summary => (summary.uuid, summary))
      .reduceByKey((agg, summary) => if (agg.lastModified < summary.lastModified) summary else agg)
      .map { case (_, summary) => OccurrenceSelector(summary.taxonSelector, summary.wktString, summary.traitSelector) }
      .collect.toSeq
  }

  def writeOccurrences(occurrences: Dataset[SelectedOccurrence], outputPath: String, saveMode: SaveMode = SaveMode.Append) = {
    import occurrences.sqlContext.implicits._

    val occurrencesMapped = occurrences.map { selOcc =>
      val occ = selOcc.occ
      val startEnd = DateUtil.startEndDate(occ.eventDate)
      val uuid = UuidUtils.uuidFor(selOcc.selector)
      val uuidString = uuid.toString
      val f0 = uuidString.substring(0, 2)
      val f1 = uuidString.substring(2, 4)
      val f2 = uuidString.substring(4, 6)
      val fmt = ISODateTimeFormat.basicDate()
      val sourceDate = fmt.withZoneUTC().parseDateTime(occ.sourceDate)
      OccurrenceHDFS(lat = occ.lat,
        lng = occ.lng,
        taxon = occ.taxonPath,
        start = startEnd._1,
        end = startEnd._2,
        id = occ.id,
        added = sourceDate.toDate.getTime,
        y = sourceDate.getYear.toString,
        m = "0" + sourceDate.getMonthOfYear takeRight 2,
        d = "0" + sourceDate.getDayOfMonth takeRight 2,
        source = occ.source,
        u0 = f0,
        u1 = f1,
        u2 = f2,
        uuid = uuidString)
    }

    occurrencesMapped
      //.coalesce(10)
      .write
      .mode(saveMode)
      .partitionBy("u0", "u1", "u2", "uuid", "y", "m", "d")
      .parquet(s"$outputPath/occurrence")


    occurrencesMapped
      .map { occSelected =>
        SourceMonitoredOccurrenceHDFS(occSelected.source,
          occSelected.id,
          occSelected.uuid,
          occSelected.y,
          occSelected.m,
          occSelected.d)
      }
      //.coalesce(10)
      .write
      .mode(saveMode)
      .partitionBy("source", "y", "m", "d")
      .parquet(s"$outputPath/source-of-monitored-occurrence")
  }

  def writeSummary(selectors: Seq[OccurrenceSelector], occurrences: Dataset[SelectedOccurrence], outputPath: String) = {
    import occurrences.sqlContext.implicits._
    val aggregate = occurrences
      .map { occSel =>
        (UuidUtils.uuidFor(occSel.selector).toString, 1L)
      }.rdd.reduceByKey((agg, value) => agg + value).toDS()

      selectors.toDS().rdd
        .map(selector => (UuidUtils.uuidFor(selector).toString, selector))
        .leftOuterJoin(aggregate.rdd)
        .map { case (uuid, (selector, count)) =>
        val f0 = uuid.substring(0, 2)
        val f1 = uuid.substring(2, 4)
        val f2 = uuid.substring(4, 6)
        OccurrenceSummaryHDFS(
          uuid = uuid,
          u0 = f0,
          u1 = f1,
          u2 = f2,
          taxonSelector = selector.taxonSelector,
          wktString = selector.wktString,
          traitSelector = selector.traitSelector,
          itemCount = count.getOrElse(0L)
        )
      }.toDS()
      //.coalesce(10)
      .write
      .mode(SaveMode.Append)
      .partitionBy("u0", "u1", "u2", "uuid")
      .parquet(s"$outputPath/$occurrenceSummaryPath")
  }
}