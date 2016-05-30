import MonitorStatusJsonProtocol._
import org.scalatest._
import spray.json._

class OccurrenceSelectors$Test extends FlatSpec with Matchers {

  "a taxon filter" should "filter taxa" in {
    val selectors = OccurrenceSelectors.all(OccurrenceSelector("Plantae", "ENVELOPE(4,5,52,50)", ""))
    val matching = OccurrenceExt("51", "4.1", "Plantae", "", 0L, "bla", 0L, 0L)
    selectors(matching) should be(true)

    val notMatching = OccurrenceExt("60", "4.1", "Plantae", "", 0L, "bla", 0L, 0L)
    selectors(notMatching) should be(false)
  }

//  ignore "a event time selector" should "include matching occurrences" in {
//    val selectors = OccurrenceSelectors.all(OccurrenceSelector("Plantae", "ENVELOPE(4,5,52,50)", "eventTime > 2016-01-01 datetime"))
//    val matching = OccurrenceExt("51", "4.1", "Plantae", "", 0L, "bla", 0L, 0L)
//    selectors(matching) should be(false)
//
//    val invertedSelectors = OccurrenceSelectors.all(OccurrenceSelector("Plantae", "ENVELOPE(4,5,52,50)", "eventTime < 2016-01-01 datetime"))
//    invertedSelectors(matching) should be(true)
//  }
//
//  "a source selector" should "include matching occurrences" in {
//    val selectors = OccurrenceSelectors.all(OccurrenceSelector("Plantae", "ENVELOPE(4,5,52,50)", "source == otherSource"))
//    val matching = OccurrenceExt("51", "4.1", "Plantae", "", 0L, "someSource", 0L, 0L)
//    selectors(matching) should be(false)
//
//    val invertedSelectors = OccurrenceSelectors.all(OccurrenceSelector("Plantae", "ENVELOPE(4,5,52,50)", "source == someSource"))
//    invertedSelectors(matching) should be(true)
//
//    val combinedSelectors = OccurrenceSelectors.all(OccurrenceSelector("Plantae", "ENVELOPE(4,5,52,50)", "source == {otherSource,someSource}"))
//    combinedSelectors(matching) should be(true)
//  }


}
