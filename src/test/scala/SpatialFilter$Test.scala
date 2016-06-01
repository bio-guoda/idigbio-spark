import org.scalatest._


class SpatialFilter$Test extends FlatSpec with Matchers {

  "a row with coordinates in envelope" should "return true" in {
    val row = Map("dwc:decimalLatitude" -> "32.5", "dwc:decimalLongitude" -> "12.3")
    SpatialFilter.locatedIn("ENVELOPE(10,13,40,30)", row) shouldBe true
  }

  "a row with coordinates in wrapped envelope" should "return true" in {
    val row = Map("dwc:decimalLatitude" -> "32.5", "dwc:decimalLongitude" -> "120")
    SpatialFilter.locatedIn("ENVELOPE(106,76,81,7)", row) shouldBe true
  }

  "a row with malformed coordinates" should "return false" in {
    val row = Map("dwc:decimalLatitude" -> "john", "dwc:decimalLongitude" -> "12.3")
    SpatialFilter.locatedIn("ENVELOPE(10,13,40,30)", row) shouldBe false
  }

  "a row without coordinates" should "return false" in {
    val row = Map("dwc:donals" -> "32.5", "dwc:mickey" -> "12.3")
    SpatialFilter.locatedIn("ENVELOPE(10,13,40,30)", row) shouldBe false
  }

  "a row with coordinates outside" should "return false" in {
    val row = Map("dwc:donals" -> "32.5", "dwc:mickey" -> "12.3")
    SpatialFilter.locatedIn("ENVELOPE(10,13,40,30)", row) shouldBe false
  }

  "an envelope covering the arctic" should "include a point on the arctic" in {
    val row = Map("dwc:decimalLatitude" -> "89.5", "dwc:decimalLongitude" -> "20.0")
    SpatialFilter.locatedIn("ENVELOPE(0.703125,-0.703125,90,65.07213008560697)", row) shouldBe true
  }

  "a polygon covering the arctic" should "include a point on the arctic" in {
    val row = Map("dwc:decimalLatitude" -> "89.5", "dwc:decimalLongitude" -> "20.0")
    SpatialFilter.locatedIn("POLYGON ((0 56.0, 0 89.9, 31.6 89.9, 31.6 56.0, 0 56.0))", row) shouldBe true
  }
  "a polygon collection covering the arctic" should "include a point on the arctic" in {
    val row = Map("dwc:decimalLatitude" -> "89.5", "dwc:decimalLongitude" -> "20.0")
    SpatialFilter.locatedIn("GEOMETRYCOLLECTION(POLYGON ((0 56.0, 0 89.9, 31.6 89.9, 31.6 56.0, 0 56.0)))", row) shouldBe true
  }

  // https://github.com/gimmefreshdata/freshdata/issues/25
  "a polygon collection beyond [-180,180] long, covering boston and guinea" should "does *not* a point in boston and guinea" in {
    val geometry: String = "GEOMETRYCOLLECTION(POLYGON ((-72 41, -72 43, -69 43, -69 41, -72 41)),POLYGON ((-219 -12, -219 0, -205 0, -205 -12, -219 -12)))"
    SpatialFilter.locatedInLatLng(geometry, Seq("42.0", "-70")) shouldBe false
    SpatialFilter.locatedInLatLng(geometry, Seq("0.0", "-210")) shouldBe false
  }

  "a polygon collection covering guinea" should "include a point in guinea, but not boston" in {
    val geometry: String = "POLYGON ((141 -12, 141 0, 155 0, 155 -12, 141 -12))"
    SpatialFilter.locatedInLatLng(geometry, Seq("42.0", "-70")) shouldBe false
    SpatialFilter.locatedInLatLng(geometry, Seq("-1.0", "142")) shouldBe true
  }

}
