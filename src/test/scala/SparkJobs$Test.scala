import java.io.IOException

import OccurrenceCollectionBuilder._
import au.com.bytecode.opencsv.CSVParser
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.effechecka.selector.OccurrenceSelector
import org.scalatest._
import org.scalatest.OptionValues._

trait TestSparkContext extends FlatSpec with Matchers with BeforeAndAfter with SharedSparkContext {

  override val conf = new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.debug.maxToStringFields", "250"). // see https://issues.apache.org/jira/browse/SPARK-15794
    set("spark.cassandra.connection.host", "localhost").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID)


}

class SparkJobs$Test extends TestSparkContext with DwCSparkHandler {

  "combining header and rows" should "create a record map" in {
    val header = new CSVParser().parseLine(traitHeader)
    val firstLine = new CSVParser().parseLine(fourTraits.head)
    val aRecord: Map[String, String] = (header.toSeq zip firstLine).toMap

    aRecord.get("Scientific Name") shouldBe Some("Balaenoptera musculus")
    aRecord.get("Value") shouldBe Some("154321.3045")
    aRecord.get("Measurement URI") shouldBe Some("http://purl.obolibrary.org/obo/VT_0001259")
    aRecord.get("Units URI (normalized)") shouldBe Some("http://purl.obolibrary.org/obo/UO_0000009")
  }

  def fourTraits: Seq[String] = {
    val fourLines = Seq(
      """328574,Balaenoptera musculus,Blue Whale,body mass,154321.3045,http://purl.obolibrary.org/obo/VT_0001259,"",kg,http://purl.obolibrary.org/obo/UO_0000009,154321304.5,g,http://purl.obolibrary.org/obo/UO_0000021,PanTHERIA,http://eol.org/content_partners/652/resources/704,"Data set supplied by Kate E. Jones. The data can also be accessed at Ecological Archives E090-184-D1, <a href=""http://esapubs.org/archive/ecol/E090/184/"">http://esapubs.org/archive/ecol/E090/184/</a>, <a href=""http://esapubs.org/archive/ecol/E090/184/PanTHERIA_1-0_WR05_Aug2008.txt"">http://esapubs.org/archive/ecol/E090/184/PanTHERIA_1-0_WR05_Aug2008.txt</a>","Kate E. Jones, Jon Bielby, Marcel Cardillo, Susanne A. Fritz, Justin O'Dell, C. David L. Orme, Kamran Safi, Wes Sechrest, Elizabeth H. Boakes, Chris Carbone, Christina Connolly, Michael J. Cutts, Janine K. Foster, Richard Grenyer, Michael Habib, Christopher A. Plaster, Samantha A. Price, Elizabeth A. Rigby, Janna Rist, Amber Teacher, Olaf R. P. Bininda-Emonds, John L. Gittleman, Georgina M. Mace, and Andy Purvis. 2009. PanTHERIA: a species-level database of life history, ecology, and geography of extant and recently extinct mammals. Ecology 90:2648.","Mass of adult (or age unspecified) live or freshly-killed specimens (excluding pregnant females) using captive, wild, provisioned, or unspecified populations; male, female, or sex unspecified individuals; primary, secondary, or extrapolated sources; all measures of central tendency; in all localities. Based on information from primary and secondary literature sources. See source for details.",average,adult,Balaenoptera musculus,,,"""
      ,
      """328577,Balaena mysticetus,Bowhead Whale,body mass,79691.17899,http://purl.obolibrary.org/obo/VT_0001259,"",kg,http://purl.obolibrary.org/obo/UO_0000009,79691178.98999999,g,http://purl.obolibrary.org/obo/UO_0000021,PanTHERIA,http://eol.org/content_partners/652/resources/704,"Data set supplied by Kate E. Jones. The data can also be accessed at Ecological Archives E090-184-D1, <a href=""http://esapubs.org/archive/ecol/E090/184/"">http://esapubs.org/archive/ecol/E090/184/</a>, <a href=""http://esapubs.org/archive/ecol/E090/184/PanTHERIA_1-0_WR05_Aug2008.txt"">http://esapubs.org/archive/ecol/E090/184/PanTHERIA_1-0_WR05_Aug2008.txt</a>","Kate E. Jones, Jon Bielby, Marcel Cardillo, Susanne A. Fritz, Justin O'Dell, C. David L. Orme, Kamran Safi, Wes Sechrest, Elizabeth H. Boakes, Chris Carbone, Christina Connolly, Michael J. Cutts, Janine K. Foster, Richard Grenyer, Michael Habib, Christopher A. Plaster, Samantha A. Price, Elizabeth A. Rigby, Janna Rist, Amber Teacher, Olaf R. P. Bininda-Emonds, John L. Gittleman, Georgina M. Mace, and Andy Purvis. 2009. PanTHERIA: a species-level database of life history, ecology, and geography of extant and recently extinct mammals. Ecology 90:2648.","Mass of adult (or age unspecified) live or freshly-killed specimens (excluding pregnant females) using captive, wild, provisioned, or unspecified populations; male, female, or sex unspecified individuals; primary, secondary, or extrapolated sources; all measures of central tendency; in all localities. Based on information from primary and secondary literature sources. See source for details.",average,adult,Balaena mysticetus,,,"""
      ,
      """222044,Sargochromis carlottae,Rainbow Happy,body mass,"1,000",http://purl.obolibrary.org/obo/VT_0001259,"",g,http://purl.obolibrary.org/obo/UO_0000021,"1,000",g,http://purl.obolibrary.org/obo/UO_0000021,FishBase,http://eol.org/content_partners/2/resources/42,"<a href=""http://www.fishbase.org/summary/SpeciesSummary.php?id=5364"">http://www.fishbase.org/summary/SpeciesSummary.php?id=5364</a>",,,max,,,,"Skelton, P.H.0 A complete guide to the freshwater fishes of southern Africa. Southern Book Publishers. 388 p. (Ref. 7248)",Susan M. Luna"""
      ,
      """1003713,Netuma thalassina,Giant Catfish,body mass,"1,000",http://purl.obolibrary.org/obo/VT_0001259,"",g,http://purl.obolibrary.org/obo/UO_0000021,"1,000",g,http://purl.obolibrary.org/obo/UO_0000021,FishBase,http://eol.org/content_partners/2/resources/42,"<a href=""http://www.fishbase.org/summary/SpeciesSummary.php?id=10220"">http://www.fishbase.org/summary/SpeciesSummary.php?id=10220</a>",,,max,,"Netuma thalassina (Rüppell, 1837)",,"Bykov, V.P.0 Marine Fishes: Chemical composition and processing properties. New Delhi: Amerind Publishing Co. Pvt. Ltd. 322 p. (Ref. 4883)",Pascualita Sa-a""")
    fourLines
  }

  "checklist" should "be filtered using trait filter" in {
    val (checklist: RDD[(String, Int)], traits: RDD[Seq[(String, String)]]) = traitsAndChecklist

    val traitSelectors: Seq[String] = """bodyMass greaterThan 1025 g|bodyMass greaterThan 1 kg""".split( """[\|,]""")

    val checklistMatchingTraits: RDD[(String, Int)] = ChecklistGenerator.filterByTraits(checklist, traits, traitSelectors)

    checklistMatchingTraits.collect().length shouldBe 1
    checklistMatchingTraits.collect() should contain( """bla | boo | Balaena mysticetus""", 23)
    checklistMatchingTraits.collect() should not contain( """bla | boo | Netuma thalassina""", 11)
  }

  "checklist with scientific name authorship" should "be filtered using trait filter" in {
    val (checklist: RDD[(String, Int)], traits: RDD[Seq[(String, String)]]) = traitsAndChecklistWithAuthorName

    val traitSelectors: Seq[String] = """bodyMass greaterThan 1025 g|bodyMass greaterThan 1 kg""".split( """[\|,]""")

    val checklistMatchingTraits: RDD[(String, Int)] = ChecklistGenerator.filterByTraits(checklist, traits, traitSelectors)

    checklistMatchingTraits.collect().length shouldBe 1
    checklistMatchingTraits.collect() should contain( """bla | boo | Balaena mysticetus (Linnaeus, 1758)""", 23)
    checklistMatchingTraits.collect() should not contain( """bla | boo | Netuma thalassina""", 11)
  }

  "checklist" should "be not filtered on empty trait filter" in {
    val (checklist: RDD[(String, Int)], traits: RDD[Seq[(String, String)]]) = traitsAndChecklist
    val checklistMatchingTraits: RDD[(String, Int)] = ChecklistGenerator.filterByTraits(checklist, traits, Seq())
    checklistMatchingTraits.collect().length shouldBe 3
    checklistMatchingTraits.collect() should contain( """bla | boo | Balaena mysticetus""", 23)
    checklistMatchingTraits.collect() should contain( """bla | boo | Netuma thalassina""", 11)
    checklistMatchingTraits.collect() should contain( """bla | boo | Mickey mousus""", 12)
  }

  "checklist" should "use dataframes to filter stuff" in {
    val occurrenceMetaDFs: Seq[(_, DataFrame)] = readDwC
    val df = occurrenceMetaDFs.head._2

    val wkt: String = "ENVELOPE(4,5,52,50)"
    val taxonNames: Seq[String] = Seq("Dactylis")

    val checklist: RDD[(String, Int)] = ChecklistBuilder.buildChecklist(sc, df, wkt, taxonNames)
    checklist.count should be(1)
  }

  "checklist" should "use dataframes to filter stuff with empty taxon selector" in {
    val occurrenceMetaDFs: Seq[(_, DataFrame)] = readDwC
    val df = occurrenceMetaDFs.head._2

    val wkt: String = "ENVELOPE(4,5,52,50)"
    val taxonNames: Seq[String] = Seq()

    val checklist: RDD[(String, Int)] = ChecklistBuilder.buildChecklist(sc, df, wkt, taxonNames)
    checklist.count should not be 0
  }

  def traitsAndChecklist: (RDD[(String, Int)], RDD[Seq[(String, String)]]) = {
    traitAndChecklist("""bla | boo | Balaena mysticetus""")
  }

  def traitsAndChecklistWithAuthorName: (RDD[(String, Int)], RDD[Seq[(String, String)]]) = {
    val firstTaxonPath = Seq("bla", "boo", "Balaena mysticetus (Linnaeus, 1758)").mkString(" | ")
    traitAndChecklist(firstTaxonPath)
  }


  def traitAndChecklist(firstTaxonPath: String): (RDD[(String, Int)], RDD[Seq[(String, String)]]) = {
    val checklist = sc.parallelize(Seq((firstTaxonPath, 23), ( """bla | boo | Netuma thalassina""", 11), ( """bla | boo | Mickey mousus""", 12)))
    val traitsRDD = sc.parallelize(fiveTraits)

    val traits = ChecklistGenerator.readRows(new CSVParser().parseLine(traitHeader), traitsRDD)
    (checklist, traits)
  }

  lazy val fiveTraits: Seq[String] = {
    Seq(
      """328574,Balaenoptera musculus,Blue Whale,body mass,154321.3045,http://purl.obolibrary.org/obo/VT_0001259,"",kg,http://purl.obolibrary.org/obo/UO_0000009,154321304.5,g,http://purl.obolibrary.org/obo/UO_0000021,PanTHERIA,http://eol.org/content_partners/652/resources/704,"Data set supplied by Kate E. Jones. The data can also be accessed at Ecological Archives E090-184-D1, <a href=""http://esapubs.org/archive/ecol/E090/184/"">http://esapubs.org/archive/ecol/E090/184/</a>, <a href=""http://esapubs.org/archive/ecol/E090/184/PanTHERIA_1-0_WR05_Aug2008.txt"">http://esapubs.org/archive/ecol/E090/184/PanTHERIA_1-0_WR05_Aug2008.txt</a>","Kate E. Jones, Jon Bielby, Marcel Cardillo, Susanne A. Fritz, Justin O'Dell, C. David L. Orme, Kamran Safi, Wes Sechrest, Elizabeth H. Boakes, Chris Carbone, Christina Connolly, Michael J. Cutts, Janine K. Foster, Richard Grenyer, Michael Habib, Christopher A. Plaster, Samantha A. Price, Elizabeth A. Rigby, Janna Rist, Amber Teacher, Olaf R. P. Bininda-Emonds, John L. Gittleman, Georgina M. Mace, and Andy Purvis. 2009. PanTHERIA: a species-level database of life history, ecology, and geography of extant and recently extinct mammals. Ecology 90:2648.","Mass of adult (or age unspecified) live or freshly-killed specimens (excluding pregnant females) using captive, wild, provisioned, or unspecified populations; male, female, or sex unspecified individuals; primary, secondary, or extrapolated sources; all measures of central tendency; in all localities. Based on information from primary and secondary literature sources. See source for details.",average,adult,Balaenoptera musculus,,,"""
      ,
      """328577,Balaena mysticetus,Bowhead Whale,body mass,79691.17899,http://purl.obolibrary.org/obo/VT_0001259,"",kg,http://purl.obolibrary.org/obo/UO_0000009,79691178.98999999,g,http://purl.obolibrary.org/obo/UO_0000021,PanTHERIA,http://eol.org/content_partners/652/resources/704,"Data set supplied by Kate E. Jones. The data can also be accessed at Ecological Archives E090-184-D1, <a href=""http://esapubs.org/archive/ecol/E090/184/"">http://esapubs.org/archive/ecol/E090/184/</a>, <a href=""http://esapubs.org/archive/ecol/E090/184/PanTHERIA_1-0_WR05_Aug2008.txt"">http://esapubs.org/archive/ecol/E090/184/PanTHERIA_1-0_WR05_Aug2008.txt</a>","Kate E. Jones, Jon Bielby, Marcel Cardillo, Susanne A. Fritz, Justin O'Dell, C. David L. Orme, Kamran Safi, Wes Sechrest, Elizabeth H. Boakes, Chris Carbone, Christina Connolly, Michael J. Cutts, Janine K. Foster, Richard Grenyer, Michael Habib, Christopher A. Plaster, Samantha A. Price, Elizabeth A. Rigby, Janna Rist, Amber Teacher, Olaf R. P. Bininda-Emonds, John L. Gittleman, Georgina M. Mace, and Andy Purvis. 2009. PanTHERIA: a species-level database of life history, ecology, and geography of extant and recently extinct mammals. Ecology 90:2648.","Mass of adult (or age unspecified) live or freshly-killed specimens (excluding pregnant females) using captive, wild, provisioned, or unspecified populations; male, female, or sex unspecified individuals; primary, secondary, or extrapolated sources; all measures of central tendency; in all localities. Based on information from primary and secondary literature sources. See source for details.",average,adult,Balaena mysticetus,,,"""
      ,
      """219907,Pseudopentaceros wheeleri,Boarfish,body mass,"1,200",http://purl.obolibrary.org/obo/VT_0001259,"",
        |g,http://purl.obolibrary.org/obo/UO_0000021,"1,200",g,http://purl.obolibrary.org/obo/UO_0000021,FishBase,
        |http://eol.org/content_partners/2/resources/42,"<a href=""http://www.fishbase.org/summary/SpeciesSummary.
        |php?id=12364"">http://www.fishbase.org/summary/SpeciesSummary.php?id=12364</a>",,,max,,"Pentaceros wheele
        |ri (Hardy, 1983)",,"Fadeev, N.S.0 Guide to biology and fisheries of fishes of the North Pacific Ocean. Vl
        |adivostok, TINRO-Center. 366 p. (Ref. 56527)",Liza Q. Agustin"""
      ,
      """222044,Sargochromis carlottae,Rainbow Happy,body mass,"1,000",http://purl.obolibrary.org/obo/VT_0001259,"",g,http://purl.obolibrary.org/obo/UO_0000021,"1,000",g,http://purl.obolibrary.org/obo/UO_0000021,FishBase,http://eol.org/content_partners/2/resources/42,"<a href=""http://www.fishbase.org/summary/SpeciesSummary.php?id=5364"">http://www.fishbase.org/summary/SpeciesSummary.php?id=5364</a>",,,max,,,,"Skelton, P.H.0 A complete guide to the freshwater fishes of southern Africa. Southern Book Publishers. 388 p. (Ref. 7248)",Susan M. Luna"""
      ,
      """1003713,Netuma thalassina,Giant Catfish,body mass,"1,000",http://purl.obolibrary.org/obo/VT_0001259,"",g,http://purl.obolibrary.org/obo/UO_0000021,"1,000",g,http://purl.obolibrary.org/obo/UO_0000021,FishBase,http://eol.org/content_partners/2/resources/42,"<a href=""http://www.fishbase.org/summary/SpeciesSummary.php?id=10220"">http://www.fishbase.org/summary/SpeciesSummary.php?id=10220</a>",,,max,,"Netuma thalassina (Rüppell, 1837)",,"Bykov, V.P.0 Marine Fishes: Chemical composition and processing properties. New Delhi: Amerind Publishing Co. Pvt. Ltd. 322 p. (Ref. 4883)",Pascualita Sa-a""")
  }

  lazy val traitHeader: String = {
    """EOL page ID,Scientific Name,Common Name,Measurement,Value,Measurement URI,Value URI,Units (normalized),Units URI (normalized),Raw Value (direct from source),Raw Units (direct from source),Raw Units URI (normalized),Supplier,Content Partner Resource URL,source,citation,measurement method,statistical method,life stage,scientific name,measurement remarks,Reference,contributor"""
  }

  "generating a checklist" should "an ordered list of most frequently observed taxa" in {
    val headers = Seq("id", "dwc:scientificName", "dwc:scientificNameAuthorship", "dwc:decimalLatitude", "dwc:decimalLongitude")
    val lines = Seq("123,Mickey mousus,walt,12.2,16.4"
      , "234,Mickey mousus,walt,12.1,17.7"
      , "234,Mickey mousus,walt,32.2,16.7"
      , "345,Donald duckus,walt,12.2,16.7"
      , "345,Donald duckus,walt,12.2,16.7"
      , "345,Donald duckus,walt,12.2,16.7"
      , "345,Donald duckus,walt,112.2,16.7"
      , "345,Donald duckus,walt,112.2,16.7"
      , "401,Mini mousus,walt,12.02,16.2")

    val rdd = sc.parallelize(lines)

    val rows = ChecklistGenerator.readRows(headers, rdd)

    val rowList: RDD[Seq[(String, String)]] = ChecklistGenerator
      .applySpatioTaxonomicFilter(rows, List("Mickey mousus", "Mini mousus", "Donald duckus"), "ENVELOPE(10,21,13,10)")

    rowList.collect should have length 6

    val checklist: RDD[(String, Int)] = ChecklistGenerator.countByTaxonAndSort(rowList)

    val checklistTop2: Array[(String, Int)] = checklist.take(2)
    checklistTop2 should have length 2
    checklistTop2 should contain("Mickey mousus", 2)
    checklistTop2 should not(contain("Mini mousus", 1))
    checklistTop2.head should be("Donald duckus", 3)

    val checklistAll = checklist.collect()
    checklistAll should have length 3
    checklistAll should contain("Mini mousus", 1)
  }


  "concatenating rows" should "be saved to cassandra" in {
    prepareCassandra()
    val otherLines = Seq(("Mammalia|Insecta", "LINE(1 2 3 4)", "bodyMass greaterThan 19 g", "checklist item", 1)
      , ("Mammalia|Insecta", "LINE(1 2 3 4)", "bodyMass greaterThan 19 g", "other checklist item", 1))

    sc.parallelize(otherLines).saveToCassandra("effechecka", "checklist", CassandraUtil.checklistColumns)

    sc.parallelize(Seq(("bla|bla", "something", "trait|anotherTrait", "running", 123L))).saveToCassandra("effechecka", "checklist_registry", CassandraUtil.checklistRegistryColumns)
  }

  "broadcast a monitor with ttl" should "serialize" in {
    val occurrenceSelectors = Seq(OccurrenceSelector("some taxa", "some wkt", "some trait", ttlSeconds = Some(123)))
    val broadcasted = sc.broadcast(occurrenceSelectors)
    occurrenceSelectors should be(broadcasted.value)
  }

  "broadcast a monitor with no ttl" should "serialize" in {
    val occurrenceSelectors = Seq(OccurrenceSelector("some taxa", "some wkt", "some trait", ttlSeconds = None))
    val broadcasted = sc.broadcast(occurrenceSelectors)
    occurrenceSelectors should be(broadcasted.value)
  }

  "occurrence selectors" should "be loaded from cassandra" in {
    prepareCassandra()
    val someSelectors = Seq(("Mammalia|Insecta", "LINE(1 2 3 4)", "bodyMass greaterThan 19 g", "status", 1)
      , ("Mammalia|Insecta", "LINE(1 2 3 4)", "bodyMass greaterThan 20 g", "other status", 1))

    val selectorsBefore: Seq[OccurrenceSelector] = new OccurrenceCollectorCassandra().occurrenceSelectorsFor(ChecklistConf(applyAllSelectors = true), sc)
    selectorsBefore.length should be(0)

    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.execute(s"INSERT INTO effechecka.monitors (taxonselector, wktstring, traitselector, accessed_at) VALUES " +
        s"('Mammalia|Insecta','LINE(1 2 3 4)','bodyMass greaterThan 19 g', dateOf(NOW()))")
      session.execute(s"INSERT INTO effechecka.monitors (taxonselector, wktstring, traitselector, accessed_at) VALUES " +
        s"('Mammalia|Insecta','LINE(1 2 3 4)','bodyMass greaterThan 20 g', dateOf(NOW())) USING TTL 100")
    }

    val selectorsAfter: Seq[OccurrenceSelector] = new OccurrenceCollectorCassandra().occurrenceSelectorsFor(ChecklistConf(applyAllSelectors = true), sc)
    selectorsAfter should contain(OccurrenceSelector("Mammalia|Insecta", "LINE(1 2 3 4)", "bodyMass greaterThan 19 g", ttlSeconds = Some(15552000)))
    selectorsAfter should not contain (OccurrenceSelector("Mammalia|Insecta", "LINE(1 2 3 4)", "bodyMass greaterThan 20 g"))

    val secondWithTtl = selectorsAfter.filter(_.ttlSeconds.isDefined).tail.head
    secondWithTtl.traitSelector should be("bodyMass greaterThan 20 g")
    secondWithTtl.ttlSeconds.value should be < 101
  }

  "occurrence selectors" should "be serializable" in {
    prepareCassandra()

    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.execute(s"INSERT INTO effechecka.monitors (taxonselector, wktstring, traitselector, accessed_at) VALUES " +
        s"('Mammalia|Insecta','LINE(1 2 3 4)','bodyMass greaterThan 19 g', dateOf(NOW()))")
      session.execute(s"INSERT INTO effechecka.monitors (taxonselector, wktstring, traitselector, accessed_at) VALUES " +
        s"('Mammalia|Insecta','LINE(1 2 3 4)','bodyMass greaterThan 20 g', dateOf(NOW())) USING TTL 100")
    }

    val selectorsAfter: Seq[OccurrenceSelector] = new OccurrenceCollectorCassandra().occurrenceSelectorsFor(ChecklistConf(applyAllSelectors = true), sc)
    selectorsAfter should be(sc.broadcast(selectorsAfter).value)
  }

  def prepareCassandra() = {
    try {
      println("preparing cassandra...")
      OccurrenceCollectorCassandraUtil.initCassandra(new SQLContext(sc))
      truncateTables()
    } catch {
      case e: IOException => {
        fail("failed to connect to cassandra. do you have it running?", e)
      }
    }
  }

  def truncateTables(): Unit = {
    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.execute(s"TRUNCATE effechecka.checklist")
      session.execute(s"TRUNCATE effechecka.monitors")
      session.execute(s"TRUNCATE effechecka.occurrence_collection_registry")
      session.execute(s"TRUNCATE effechecka.occurrence_collection")
      session.execute(s"TRUNCATE effechecka.occurrence_search")
      session.execute(s"TRUNCATE effechecka.occurrence_first_added_search")
    }
  }

  def insertSomeSearchResults() = {
    prepareCassandra()

    val otherLines = Seq(
      ("some taxonselector", "some wktstring", "some traitselector", "Animalia|Aves", "11.4", "12.2", "2013-05-03", 123L, 124L, 635829854630400000L, "http://archive2")
      , ("some taxonselector", "some wktstring", "some traitselector", "Animalia|Aves", "11.4", "12.2", "2013-05-03", 123L, 125L, 635829854630400000L, "http://archive2")
      , ("some other taxonselector", "some wktstring", "some traitselector", "Animalia|Aves", "11.4", "12.2", "2013-05-03", 123L, 125L, 635829854630400000L, "http://archive2")
    )
    sc.parallelize(otherLines).saveToCassandra("effechecka", "occurrence_collection", CassandraUtil.occurrenceCollectionColumns)
  }

  def plantaeSelector = Seq(OccurrenceSelector("Plantae", "ENVELOPE(4,5,52,50)", ""))

  def dactylisSelector = Seq(OccurrenceSelector("Dactylis", "ENVELOPE(4,5,52,50)", ""))

  "apply occurrence filter to gbif sample" should "select a few occurrences" in {
    val sqlContext = new SQLContext(sc)

    val gbif = readDwC.head._2

    val gbifOcc: Dataset[Occurrence] = toOccurrenceDS(sqlContext, gbif)
    val collection = selectOccurrences(sqlContext, gbifOcc, plantaeSelector)

    val selectedOccurrences = collection.collect()
    selectedOccurrences.size should be(9)
    selectedOccurrences.map(_.occ.lat) should contain("51.94536")

    val anotherCollection = selectOccurrences(sqlContext, gbifOcc, dactylisSelector)

    anotherCollection.count() should be(1)

  }

  "apply first added aggregate" should "select a few occurrences" in {
    val sqlContext = new SQLContext(sc)

    val df = readDwCNoSource.head._2

    val gbif2010 = df.withColumn("date", lit("20100101")).withColumn("source", lit("gbif"))
    val gbif2012 = df.withColumn("date", lit("20120101")).withColumn("source", lit("gbif"))

    val gbif = gbif2010.unionAll(gbif2012)

    val gbifOcc: Dataset[Occurrence] = toOccurrenceDS(sqlContext, gbif)

    val twoSelectors = Seq(OccurrenceSelector("Plantae", "ENVELOPE(4,5,52,50)", ""),
      OccurrenceSelector("Plantae", "ENVELOPE(4.1,5,52,50)", ""))

    val collection = selectOccurrences(sqlContext, gbifOcc, twoSelectors)

    collection.count() should be(36)

    val gbifFirstSeenOnly = firstSeenOccurrences(sqlContext, collection)
    gbifFirstSeenOnly.first().occ.sourceDate should be("20100101")
    gbifFirstSeenOnly.count() should be(18)

    val anotherCollection = selectOccurrences(sqlContext, gbifOcc, dactylisSelector)
    anotherCollection.count() should be(2)

  }

  "apply occurrence filter to idigbio sample" should "select a few occurrences" in {
    val idigbio = readDwC.last._2
    val sqlContext = new SQLContext(sc)

    val selectors = Seq(OccurrenceSelector("Animalia", "ENVELOPE(-100,-90,40,30)", ""))
    val collection = buildOccurrenceCollection(sc, toOccurrenceDS(sqlContext, idigbio), selectors)

    collection.count() should be(1)
    collection.first().occ.lat should be("33.4519400")

    val otherSelectors = Seq(OccurrenceSelector("Crurithyris", "ENVELOPE(-100,-90,40,30)", ""))

    val anotherCollection = buildOccurrenceCollection(sc, toOccurrenceDS(sqlContext, idigbio), otherSelectors)

    anotherCollection.count() should be(1)

  }


  "combining metas" should "turn up with aggregated records" in {
    val occurrenceMetaDFs: Seq[(_, DataFrame)] = readDwC

    val occurrenceDFs = occurrenceMetaDFs map (_._2)

    occurrenceDFs.length should be(2)

    occurrenceDFs.head.columns should contain("http://rs.gbif.org/terms/1.0/gbifID")
    occurrenceDFs.last.columns should contain("undefined0")
    occurrenceDFs.foreach {
      _.columns should contain("http://rs.tdwg.org/dwc/terms/scientificName")
    }

  }


  def readDwC: Seq[(String, DataFrame)] = {
    readDwCNoSource.map {
      fileDF => (fileDF._1,
        fileDF._2.withColumn("date", date_format(current_date(), "yyyyMMdd")).withColumn("source", col("`http://rs.tdwg.org/dwc/terms/institutionCode`")))
    }
  }

  def readDwCNoSource: Seq[(String, DataFrame)] = {
    val sqlContext = new SQLContext(sc)
    val metas = List("/gbif/meta.xml", "/idigbio/meta.xml") map {
      getClass.getResource
    }
    toDF2(sqlContext, metas map {
      _.toString
    })
  }

  "occurrence collection" should "be saved to cassandra" in {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    prepareCassandra()

    val occurrences = Seq(OccurrenceCassandra(lat = "11.4", lng = "12.2",
      taxon = "Animalia|Aves", added = 123L, start = 44L, end = 55L,
      id = "some id", source = "some data source",
      taxonselector = "some taxonselector", wktstring = "some wktstring", traitselector = "some traitselector"))

    new OccurrenceCollectorCassandra().saveCollectionToCassandra(sqlContext,
      occurrenceCollection = sqlContext.createDataset(occurrences))

    val df = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "occurrence_collection", "keyspace" -> "effechecka"))
      .load()

    df.count() should be(1)

    val searches = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "occurrence_search", "keyspace" -> "effechecka"))
      .load()

    searches.first should be(Row("some data source", "some id", "some taxonselector", "some wktstring", "some traitselector"))

    val searchesFirstAdded = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "occurrence_first_added_search", "keyspace" -> "effechecka"))
      .load()

    searchesFirstAdded.first.getAs[String]("source") should be("some data source")
    searchesFirstAdded.first.getAs[String]("id") should be("some id")

  }

}
