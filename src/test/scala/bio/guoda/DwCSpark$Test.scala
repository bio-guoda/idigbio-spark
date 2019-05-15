package bio.guoda

import org.apache.spark.sql.SparkSession



class DwCSpark$Test extends TestSparkContext {

  override implicit def reuseContextIfPossible: Boolean = false

  "a meta.xml" should "help create a dataset with defined schema" in {
    implicit val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
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
    implicit val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
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
