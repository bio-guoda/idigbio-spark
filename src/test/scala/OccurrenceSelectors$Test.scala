import org.scalatest._
import OccurrenceSelectors._

class OccurrenceSelectors$Test extends FlatSpec with Matchers {

  "a taxon filter" should "filter taxa" in {
    val selectors = apply(OccurrenceSelector("Plantae", "ENVELOPE(4,5,52,50)", ""))
    val matching = Occurrence("51", "4.1", "Plantae", "2015-01-01", "someId", "20140101", "someSource")
    selectors(matching) should be(true)

    val notMatching = Occurrence("60", "4.1", "Plantae", "2015-01-01", "someId", "20140101", "someSource")
    selectors(notMatching) should be(false)
  }

  "a taxon filter" should "filter taxa by partials" in {
    val selectors = apply(OccurrenceSelector("Aedes", "ENVELOPE(4,5,52,50)", ""))
    val matching = Occurrence("51", "4.1", "Insecta|Aedes mitchellae", "2015-01-01", "someId", "20140101", "someSource")
    selectors(matching) should be(true)

    val notMatching = Occurrence("51", "4.1", "Insecta|Aedes|Aedes mitchellae", "2015-01-01", "someId", "20140101", "someSource")
    selectors(notMatching) should be(true)
  }

  "a taxon filter" should "filter taxa by ignore case" in {
    val selectors = apply(OccurrenceSelector("aedes", "ENVELOPE(4,5,52,50)", ""))
    val matching = Occurrence("51", "4.1", "Insecta|Aedes mitchellae", "2015-01-01", "someId", "20140101", "someSource")
    selectors(matching) should be(true)
  }

  "a taxon filter" should "filter taxa by ignore case 2" in {
    val selectors = apply(OccurrenceSelector("Aedes", "ENVELOPE(4,5,52,50)", ""))
    val matching = Occurrence("51", "4.1", "Insecta|aedes mitchellae", "2015-01-01", "someId", "20140101", "someSource")
    selectors(matching) should be(true)
  }

  "a event time selector" should "include matching occurrences" in {
    val selectors = traitSelector(OccurrenceSelector(traitSelector = "eventDate > 2016-01-01 datetime"))
    val matching = Occurrence("60", "4.1", "Plantae", "2017-01-01", "someId", "20140101", "someSource")
    selectors(matching) should be(true)

    val invertedSelectors = traitSelector(OccurrenceSelector(traitSelector = "eventDate < 2016-01-01 datetime"))
    invertedSelectors(matching) should be(false)
  }

  "a source selector" should "include matching occurrences" in {
    val selectors = traitSelector(OccurrenceSelector(traitSelector = "source == otherSource string"))
    val matching = Occurrence("60", "4.1", "Plantae", "2015-01-01", "someId", "20140101", "someSource")
    selectors(matching) should be(false)

    val invertedSelectors = traitSelector(OccurrenceSelector(traitSelector = "source == someSource string"))
    invertedSelectors(matching) should be(true)

    val notSelector = traitSelector(OccurrenceSelector(traitSelector = "source != otherSource string"))
    notSelector(matching) should be(true)
  }


}
