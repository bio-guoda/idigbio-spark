import org.scalatest._
import OccurrenceSelectors._

class TraitSelectorParser$Test extends FlatSpec with Matchers {

  val occTest = Occurrence("51", "4.1", "Plantae", "2015-01-01/2015-01-02", "someId", "20140101", "someSource")

  "parser" should "produce a trait filter config with >" in {
    val selectorString: String = "eventDate > 2014-02-01 datetime"
    parse(selectorString)(occTest) should be(true)
  }

  "parser" should "produce a trait filter config with <" in {
    val selectorString: String = "eventDate < 2015-04-01 datetime"
    parse(selectorString)(occTest) should be(true)
  }

  "parser" should "produce a trait filter config with < and >" in {
    val selectorString: String = "eventDate < 2015-02-01 datetime | eventDate > 2014-01-01 datetime"
    parse(selectorString)(occTest) should be(true)
  }

  "parser" should "produce a trait filter config with < missing unit" in {
    val selectorString: String = "eventDate < 2015-02-01"
    TraitSelectorParser.parse(TraitSelectorParser.config, selectorString) match {
      case TraitSelectorParser.Success(traitConfig, _) => fail("didn't expect success, missing unit")
      case TraitSelectorParser.Failure(msg, _) =>
      case TraitSelectorParser.Error(msg, _) =>
    }
  }

  "parser" should "produce a trait filter config with syntax error" in {
    val selectorString: String = "donald duck went to town"
    TraitSelectorParser.parse(TraitSelectorParser.config, selectorString) match {
      case TraitSelectorParser.Success(traitConfig, _) => fail("didn't expect success")
      case TraitSelectorParser.Failure(msg, _) =>
      case TraitSelectorParser.Error(msg, _) =>
    }
  }


  def parse(selectorString: String): OccurrenceFilter = {
    TraitSelectorParser.parse(TraitSelectorParser.config, selectorString).get
  }
}
