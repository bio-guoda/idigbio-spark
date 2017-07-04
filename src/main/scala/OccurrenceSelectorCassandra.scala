import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SQLContext, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.effechecka.selector.OccurrenceSelector
import com.datastax.spark.connector.writer.{TTLOption, WriteConf}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._

import scala.collection.JavaConversions

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

object OccurrenceCollectorCassandraUtil {
  def initCassandra(sqlContext: SQLContext): Unit = {
    CassandraConnector(sqlContext.sparkContext.getConf).withSessionDo { session =>
      session.execute(CassandraUtil.checklistKeySpaceCreate)
      session.execute(CassandraUtil.checklistTableCreate)
      session.execute(CassandraUtil.checklistRegistryTableCreate)
      session.execute(CassandraUtil.occurrenceCollectionRegistryTableCreate)
      session.execute(CassandraUtil.occurrenceCollectionTableCreate)
      session.execute(CassandraUtil.occurrenceSearchTableCreate)
      session.execute(CassandraUtil.occurrenceFirstAddedSearchTableCreate)
      session.execute(CassandraUtil.monitorsTableCreate)
    }
  }
}

class OccurrenceCollectorCassandra extends OccurrenceCollector {
  def write(config: ChecklistConf, sc: SparkContext): Unit = writeCassandra(config, sc)

  private def writeCassandra(config: ChecklistConf, sc: SparkContext) = {
    val occurrenceSelectors = occurrenceSelectorsFor(config, sc)
    val selectors: Broadcast[Seq[OccurrenceSelector]] = sc.broadcast(occurrenceSelectors)
    val sqlContext = SQLContextSingleton.getInstance(sc)
    import sqlContext.implicits._
    val occurrenceCollection: Dataset[SelectedOccurrence] = occurrenceCollectionsFor(config, sqlContext, selectors)

    OccurrenceCollectorCassandraUtil.initCassandra(sqlContext)
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

    val normalizedOccurrenceCollection = occurrenceCollection
      .transform(normalize).persist(StorageLevel.MEMORY_AND_DISK)
    selectors.value.foreach(selector => {
      SparkUtil.logInfo(s"saving [$selector] to cassandra...")
      val occForSelector = normalizedOccurrenceCollection.filter(occ =>
        occ.taxonselector == selector.taxonSelector
          && occ.wktstring == selector.wktString
          && occ.traitselector == selector.traitSelector)

      saveCollectionToCassandra(sqlContext = sqlContext, occurrenceCollection = occForSelector, ttl = selector.ttlSeconds)

      val countBySelector: Long = occForSelector.count()

      val writeConf = selector.ttlSeconds match {
        case Some(ttlSecondsValue) => WriteConf(ttl = TTLOption.constant(ttlSecondsValue))
        case None => WriteConf()
      }

      sqlContext.sparkContext.parallelize(Seq((selector.taxonSelector, selector.wktString, selector.traitSelector, "ready", countBySelector)))
        .saveToCassandra("effechecka", "occurrence_collection_registry", CassandraUtil.occurrenceCollectionRegistryColumns, writeConf = writeConf)
      SparkUtil.logInfo(s"saved [$selector].")
    }
    )
  }




  def occurrenceSelectorsFor(config: ChecklistConf, sc: SparkContext): Seq[OccurrenceSelector] = {
    val sqlString: String = "SELECT taxonselector, wktstring, traitselector, TTL(accessed_at) FROM effechecka.monitors"
    val (query, params) = if (config.applyAllSelectors) {
      (sqlString, List())
    } else {
      val selectorConfig = OccurrenceSelectors.toOccurrenceSelector(config)
      val withWhereClause = s"$sqlString WHERE taxonselector = ? AND wktstring = ? AND traitselector = ?"
      (withWhereClause, List(selectorConfig.taxonSelector, selectorConfig.wktString, selectorConfig.traitSelector))
    }

    val session = CassandraConnector(sc.getConf).openSession()
    val result = session.execute(query, params: _*)

    val rows = JavaConversions.collectionAsScalaIterable(result.all())
    rows.map(row => {
      val ttlSeconds = if (row.isNull(3)) {
        Some(3600 * 24 * 180) // default 180 days
      } else {
        Some(row.getInt(3))
      }
      OccurrenceSelector(row.getString(0), row.getString(1), row.getString(2), ttlSeconds = ttlSeconds)
    }
    ).toSeq
  }

  def saveCollectionToCassandra(sqlContext: SQLContext, occurrenceCollection: Dataset[OccurrenceCassandra], ttl: Option[Int] = None): Unit = {
    import sqlContext.implicits._

    def saveIntoTable[T](ds: Dataset[T], tableName: String): Unit = {
      val defaultMap = Map("keyspace" -> "effechecka")
      val writeConfig = ttl match {
        case Some(ttlSeconds) => defaultMap ++ Map("spark.cassandra.output.ttl" -> s"$ttlSeconds") // see https://github.com/gimmefreshdata/freshdata/issues/32
        case _ => defaultMap
      }

      ds.toDF()
        .write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> tableName) ++ writeConfig)
        .mode(SaveMode.Append)
        .save()
    }

    saveIntoTable[OccurrenceCassandra](occurrenceCollection, "occurrence_collection")

    saveIntoTable[MonitoredOccurrence](occurrenceCollection.map(item => {
      MonitoredOccurrence(source = item.source,
        id = item.id,
        taxonselector = item.taxonselector,
        wktstring = item.wktstring,
        traitselector = item.traitselector)
    }), "occurrence_search")

    saveIntoTable[FirstOccurrence](occurrenceCollection.map(item => {
      FirstOccurrence(source = item.source, added = item.added, id = item.id)
    }), "occurrence_first_added_search")
  }

}