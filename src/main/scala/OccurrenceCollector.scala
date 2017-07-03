import OccurrenceCollectionGenerator.load
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLContext
import org.effechecka.selector.OccurrenceSelector

trait OccurrenceCollector {

  def write(config: ChecklistConf, sc: SparkContext)

  def occurrenceCollectionsFor(config: ChecklistConf, sqlContext: SQLContext, selectors: Broadcast[Seq[OccurrenceSelector]]) = {
    import sqlContext.implicits._
    val applySelectors = buildOccurrenceSelector(config)
    val occurrenceFile = config.occurrenceFiles.head
    val occurrenceCollection = load(occurrenceFile, sqlContext).transform[SelectedOccurrence]({ ds =>
      applySelectors(sqlContext, ds, selectors.value)
    })
    occurrenceCollection
  }

  private def buildOccurrenceSelector(config: ChecklistConf) = {
    if (config.firstSeenOnly) {
      OccurrenceCollectionBuilder.selectOccurrencesFirstSeenOnly _
    } else {
      OccurrenceCollectionBuilder.selectOccurrences _
    }
  }

}
