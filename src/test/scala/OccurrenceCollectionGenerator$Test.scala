import org.scalatest._

class OccurrenceCollectionGenerator$Test extends FlatSpec with Matchers {

  "calling tool" should "print something" in {
    val args: Array[String] = Array("-c", "archive1|archive2|archive3", "-t", "traitArchive1", "-f", "some output format")
    OccurrenceCollectionGenerator.config(args ++ Array("taxonA|taxonB", "some wkt string", "trait1|trait2")) match {
      case Some(config) => {
        config.occurrenceFiles.size should be(3)
        config.occurrenceFiles should contain("archive1")
        config.traitFiles should contain("traitArchive1")
        config.outputFormat should be("some output format")
        config.geoSpatialSelector should be("some wkt string")
        config.taxonSelector should be(Seq("taxonA", "taxonB"))
        config.traitSelector should be(Seq("trait1", "trait2"))
        config.applyAllSelectors should be(false)
      }
      case None => fail("should return a valid config object")
    }
  }

  "calling tool without selectors" should "be ok" in {
    val args: Array[String] = Array("-c", "archive1|archive2|archive3", "-t", "traitArchive1", "-f", "some output format", "-a", "true")
    OccurrenceCollectionGenerator.config(args) match {
      case Some(config) => {
        config.occurrenceFiles.size should be(3)
        config.occurrenceFiles should contain("archive1")
        config.traitFiles should contain("traitArchive1")
        config.outputFormat should be("some output format")
        config.geoSpatialSelector should be("")
        config.taxonSelector should be(Seq())
        config.traitSelector should be(Seq())
        config.applyAllSelectors should be(true)
      }
      case None => fail("should return a valid config object")
    }
  }

  "calling tool" should "print something also" in {
    OccurrenceCollectionGenerator.config(Array()) match {
      case Some(config) => {
        fail("should return a invalid config object")
      }
      case None => {

      }
    }
  }

}