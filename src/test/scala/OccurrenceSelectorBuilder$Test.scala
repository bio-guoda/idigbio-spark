import MonitorStatusJsonProtocol._
import org.scalatest._
import spray.json._


class OccurrenceSelectorBuilder$Test extends FlatSpec with Matchers {

  "a taxon filter" should "filter taxa" in {
    val selectors = OccurrenceCollectionGenerator.buildSelectors(OccurrenceSelector("Plantae", "ENVELOPE(4,5,52,50)", ""))
    val matching = OccurrenceExt("51", "4.1", "Plantae", "", 0L, "bla", 0L, 0L)
    selectors.forall(_(matching) == true) should be(true)

    val notMatching = OccurrenceExt("60", "4.1", "Plantae", "", 0L, "bla", 0L, 0L)
    selectors.forall(_(notMatching) == true) should be(false)
  }



}
