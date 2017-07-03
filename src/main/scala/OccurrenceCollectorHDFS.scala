import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset
import org.effechecka.selector.OccurrenceSelector

class OccurrenceCollectorHDFS extends OccurrenceCollector {

  def write(config: ChecklistConf, sc: SparkContext): Unit = {
    val occurrenceSelectors = Seq(OccurrenceSelectors.toOccurrenceSelector(config))
    val selectors: Broadcast[Seq[OccurrenceSelector]] = sc.broadcast(occurrenceSelectors)
    val sqlContext = SQLContextSingleton.getInstance(sc)
    val occurrenceCollection: Dataset[SelectedOccurrence] = occurrenceCollectionsFor(config, sqlContext, selectors)
  }
}