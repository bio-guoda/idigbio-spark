package bio.guoda

import bio.guoda.preston.spark.TestSparkContext
import org.apache.spark.sql.SparkSession



class DwCSpark$Test extends TestSparkContext {

  override implicit def reuseContextIfPossible: Boolean = true

  "unzip a zip entry" should "unpack and bzip2 compress the entries" in {
    implicit val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val metaOption = DwC.readMeta(getClass.getResource("/inaturalist/meta.xml").toURI)
    val ds = DwC.toDS(metaOption.get, metaOption.get.fileURIs, spark)
    import spark.implicits._
    val kingdoms = ds.select("`http://rs.tdwg.org/dwc/terms/kingdom`").as[String]

    kingdoms.count should be(1)
    kingdoms.first() should be("Animalia")
  }

}
