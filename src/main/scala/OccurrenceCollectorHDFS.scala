import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SQLContext, SaveMode}
import org.apache.spark.storage.StorageLevel
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

case class SourceMonitoredOccurrenceHDFS(source: String, id: String)

case class MonitorsOfOccurrenceHDFS(uuid: String, source: String, u0: String, u1: String, u2: String, monitorUUID: String)

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
    writeToParquet(occurrenceCollection.persist(StorageLevel.MEMORY_AND_DISK), config.outputPath, saveMode)
  }

  def allSelectorsFor(sqlContext: SQLContext, outputPath: String) = {
    import sqlContext.implicits._
    sqlContext.read.parquet(s"$outputPath/$occurrenceSummaryPath")
      .as[OccurrenceSummaryHDFS]
      .rdd.map(summary => (summary.uuid, summary))
      .reduceByKey((agg, summary) => if (agg.lastModified < summary.lastModified) summary else agg)
      .map { case (_, summary) => OccurrenceSelector(summary.taxonSelector, summary.wktString, summary.traitSelector) }
      .collect.toSeq
  }

  def writeToParquet(occurrences: Dataset[SelectedOccurrence], outputPath: String, saveMode: SaveMode = SaveMode.Append) = {
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
        y = s"${sourceDate.getYear}",
        m = f"${sourceDate.getMonthOfYear}%02d",
        d = f"${sourceDate.getDayOfMonth}%02d",
        source = occ.source,
        u0 = f0,
        u1 = f1,
        u2 = f2,
        uuid = uuidString)
    }

    occurrencesMapped
      .coalesce(10)
      .write
      .mode(saveMode)
      .partitionBy("u0", "u1", "u2", "uuid", "y", "m", "d")
      .parquet(s"$outputPath/occurrence")

    occurrencesMapped
      .map { occ =>
        val uuid = UuidUtils.generator.generate(occ.id)
        val uuidString = uuid.toString
        val f0 = uuidString.substring(0, 2)
        val f1 = uuidString.substring(2, 4)
        val f2 = uuidString.substring(4, 6)
        MonitorsOfOccurrenceHDFS(
          uuid = uuidString,
          source = occ.source,
          u0 = f0,
          u1 = f1,
          u2 = f2,
          monitorUUID = occ.uuid
        )
      }
      .coalesce(1)
      .write
      .mode(saveMode)
      .partitionBy("u0", "u1", "u2", "uuid")
      .parquet(s"$outputPath/monitor-of-occurrence")

    occurrencesMapped
      .map { occ =>
        SourceMonitoredOccurrenceHDFS(occ.source, occ.id)
      }
      .coalesce(10)
      .write
      .mode(saveMode)
      .partitionBy("source")
      .parquet(s"$outputPath/source-of-monitored-occurrence")

    occurrences
      .map { occSel =>
        val uuid = UuidUtils.uuidFor(occSel.selector)
        val uuidString = uuid.toString
        (uuidString, (occSel.selector, 1))
      }
      .rdd.reduceByKey((agg, value) => (agg._1, agg._2 + value._2)).toDS()
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
          itemCount = count
        )
      }
      .coalesce(10)
      .write
      .mode(SaveMode.Append)
      .partitionBy("u0", "u1", "u2", "uuid")
      .parquet(s"$outputPath/$occurrenceSummaryPath")
  }
}