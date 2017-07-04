import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SaveMode}
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
                                 count: Long,
                                 status: String = "ready",
                                 lastModified: Long = new Date().getTime
                                )


class OccurrenceCollectorHDFS extends OccurrenceCollector {

  def write(config: ChecklistConf, sc: SparkContext): Unit = {
    val (saveMode, occurrenceSelectors) = if (config.applyAllSelectors) {
      (SaveMode.Overwrite, Seq())
    } else {
      (SaveMode.Append, Seq(OccurrenceSelectors.toOccurrenceSelector(config)))
    }

    val selectors: Broadcast[Seq[OccurrenceSelector]] = sc.broadcast(occurrenceSelectors)
    val sqlContext = SQLContextSingleton.getInstance(sc)
    val occurrenceCollection: Dataset[SelectedOccurrence] = occurrenceCollectionsFor(config, sqlContext, selectors)
    writeToParquet(occurrenceCollection, config.outputPath, saveMode)
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

    occurrencesMapped.write
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
      .write
      .mode(saveMode)
      .partitionBy("u0", "u1", "u2", "uuid")
      .parquet(s"$outputPath/monitor-of-occurrence")

    occurrencesMapped
      .map { occ =>
        SourceMonitoredOccurrenceHDFS(occ.source, occ.id)
      }
      .write
      .mode(saveMode)
      .partitionBy("source")
      .parquet(s"$outputPath/source-of-monitored-occurrence")

    occurrences
      .map { occSel =>
        val uuid = UuidUtils.uuidFor(occSel.selector)
        val uuidString = uuid.toString
        (uuidString, 1)
      }
      .rdd.reduceByKey(_ + _).toDS()
      .map { case (uuid, count) =>
        val f0 = uuid.substring(0, 2)
        val f1 = uuid.substring(2, 4)
        val f2 = uuid.substring(4, 6)
        OccurrenceSummaryHDFS(
          uuid = uuid,
          u0 = f0,
          u1 = f1,
          u2 = f2,
          count = count
        )
      }
      .write
      .mode(saveMode)
      .partitionBy("u0", "u1", "u2", "uuid")
      .parquet(s"$outputPath/occurrence-summary")
  }
}