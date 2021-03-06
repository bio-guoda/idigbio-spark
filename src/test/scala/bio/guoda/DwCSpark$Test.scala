package bio.guoda



class DwCSpark$Test extends TestSparkContext {

  "a meta.xml" should "help create a dataset with defined schema" in {
    val spark = sparkSession
    val metaOption = DwC.readMeta(getClass.getResource("/inaturalist/meta.xml").toURI)
    val ds = DwC.toDS(metaOption.get, metaOption.get.fileURIs, spark)
    import spark.implicits._
    val kingdoms = ds.select("`http://rs.tdwg.org/dwc/terms/kingdom`").as[String]

    kingdoms.count should be(1)
    kingdoms.first() should be("Animalia")

    val bad_records = ds.select("bad_record").as[String]

    bad_records.count should be(1)
    bad_records.first() should be(null)
  }

  "bad records" should "be stored in a separate column" in {
    val spark = sparkSession
    val metaOption = DwC.readMeta(getClass.getResource("/badrecord/meta.xml").toURI)
    val ds = DwC.toDS(metaOption.get, metaOption.get.fileURIs, spark)
    import spark.implicits._
    val kingdoms = ds.select("`http://rs.tdwg.org/dwc/terms/kingdom`").as[String]

    kingdoms.count should be(2)
    kingdoms.filter(_ != null).first() should be("Animalia")

    val bad_records = ds.select("bad_record").as[String]

    bad_records.count should be(2)
    bad_records.filter(_ != null).first() should be("this is an invalid record")
  }

}
