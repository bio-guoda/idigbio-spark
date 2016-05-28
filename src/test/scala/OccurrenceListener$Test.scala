import org.scalatest._
import spray.json._
import MonitorStatusJsonProtocol._


class OccurrenceListener$Test extends FlatSpec with Matchers {

  "config2string" should "be a json object" in {
    val status = MonitorStatus(OccurrenceSelector("someTaxonSelector", "someWktString", "someTraitSelector"),
      status = "processing", percentComplete = 102.2, eta = 10L)

    status.toJson.toString should be("""{"selector":{"taxonSelector":"someTaxonSelector","wktString":"someWktString","traitSelector":"someTraitSelector"},"status":"processing","percentComplete":102.2,"eta":10}""")
  }


}
