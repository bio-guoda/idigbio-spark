import java.io.IOException

import au.com.bytecode.opencsv.CSVParser
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

class SparkJobs$Test extends FlatSpec with BeforeAndAfterAll with Matchers {

  private var sc: SparkContext = _

  override def beforeAll() = {
    val conf = new SparkConf()
      .set("spark.cassandra.connection.host", "localhost")
      .setMaster("local[2]")
      .setAppName("test-spark")
    sc = new SparkContext(conf)
  }

  override def afterAll() = {
    if (sc != null) {
      sc.stop()
    }
  }

  "combining header and rows" should "create a record map" in {
    val headers = """EOL page ID,Scientific Name,Common Name,Measurement,Value,Measurement URI,Value URI,Units (normalized),Units URI (normalized),Raw Value (direct from source),Raw Units (direct from source),Raw Units URI (normalized),Supplier,Content Partner Resource URL,source,citation,measurement method,statistical method,life stage,scientific name,measurement remarks,Reference,contributor"""
    val fourLines = Seq( """328574,Balaenoptera musculus,Blue Whale,body mass,154321.3045,http://purl.obolibrary.org/obo/VT_0001259,"",kg,http://purl.obolibrary.org/obo/UO_0000009,154321304.5,g,http://purl.obolibrary.org/obo/UO_0000021,PanTHERIA,http://eol.org/content_partners/652/resources/704,"Data set supplied by Kate E. Jones. The data can also be accessed at Ecological Archives E090-184-D1, <a href=""http://esapubs.org/archive/ecol/E090/184/"">http://esapubs.org/archive/ecol/E090/184/</a>, <a href=""http://esapubs.org/archive/ecol/E090/184/PanTHERIA_1-0_WR05_Aug2008.txt"">http://esapubs.org/archive/ecol/E090/184/PanTHERIA_1-0_WR05_Aug2008.txt</a>","Kate E. Jones, Jon Bielby, Marcel Cardillo, Susanne A. Fritz, Justin O'Dell, C. David L. Orme, Kamran Safi, Wes Sechrest, Elizabeth H. Boakes, Chris Carbone, Christina Connolly, Michael J. Cutts, Janine K. Foster, Richard Grenyer, Michael Habib, Christopher A. Plaster, Samantha A. Price, Elizabeth A. Rigby, Janna Rist, Amber Teacher, Olaf R. P. Bininda-Emonds, John L. Gittleman, Georgina M. Mace, and Andy Purvis. 2009. PanTHERIA: a species-level database of life history, ecology, and geography of extant and recently extinct mammals. Ecology 90:2648.","Mass of adult (or age unspecified) live or freshly-killed specimens (excluding pregnant females) using captive, wild, provisioned, or unspecified populations; male, female, or sex unspecified individuals; primary, secondary, or extrapolated sources; all measures of central tendency; in all localities. Based on information from primary and secondary literature sources. See source for details.",average,adult,Balaenoptera musculus,,,"""
      , """328577,Balaena mysticetus,Bowhead Whale,body mass,79691.17899,http://purl.obolibrary.org/obo/VT_0001259,"",kg,http://purl.obolibrary.org/obo/UO_0000009,79691178.98999999,g,http://purl.obolibrary.org/obo/UO_0000021,PanTHERIA,http://eol.org/content_partners/652/resources/704,"Data set supplied by Kate E. Jones. The data can also be accessed at Ecological Archives E090-184-D1, <a href=""http://esapubs.org/archive/ecol/E090/184/"">http://esapubs.org/archive/ecol/E090/184/</a>, <a href=""http://esapubs.org/archive/ecol/E090/184/PanTHERIA_1-0_WR05_Aug2008.txt"">http://esapubs.org/archive/ecol/E090/184/PanTHERIA_1-0_WR05_Aug2008.txt</a>","Kate E. Jones, Jon Bielby, Marcel Cardillo, Susanne A. Fritz, Justin O'Dell, C. David L. Orme, Kamran Safi, Wes Sechrest, Elizabeth H. Boakes, Chris Carbone, Christina Connolly, Michael J. Cutts, Janine K. Foster, Richard Grenyer, Michael Habib, Christopher A. Plaster, Samantha A. Price, Elizabeth A. Rigby, Janna Rist, Amber Teacher, Olaf R. P. Bininda-Emonds, John L. Gittleman, Georgina M. Mace, and Andy Purvis. 2009. PanTHERIA: a species-level database of life history, ecology, and geography of extant and recently extinct mammals. Ecology 90:2648.","Mass of adult (or age unspecified) live or freshly-killed specimens (excluding pregnant females) using captive, wild, provisioned, or unspecified populations; male, female, or sex unspecified individuals; primary, secondary, or extrapolated sources; all measures of central tendency; in all localities. Based on information from primary and secondary literature sources. See source for details.",average,adult,Balaena mysticetus,,,"""
      , """222044,Sargochromis carlottae,Rainbow Happy,body mass,"1,000",http://purl.obolibrary.org/obo/VT_0001259,"",g,http://purl.obolibrary.org/obo/UO_0000021,"1,000",g,http://purl.obolibrary.org/obo/UO_0000021,FishBase,http://eol.org/content_partners/2/resources/42,"<a href=""http://www.fishbase.org/summary/SpeciesSummary.php?id=5364"">http://www.fishbase.org/summary/SpeciesSummary.php?id=5364</a>",,,max,,,,"Skelton, P.H.0 A complete guide to the freshwater fishes of southern Africa. Southern Book Publishers. 388 p. (Ref. 7248)",Susan M. Luna"""
      , """1003713,Netuma thalassina,Giant Catfish,body mass,"1,000",http://purl.obolibrary.org/obo/VT_0001259,"",g,http://purl.obolibrary.org/obo/UO_0000021,"1,000",g,http://purl.obolibrary.org/obo/UO_0000021,FishBase,http://eol.org/content_partners/2/resources/42,"<a href=""http://www.fishbase.org/summary/SpeciesSummary.php?id=10220"">http://www.fishbase.org/summary/SpeciesSummary.php?id=10220</a>",,,max,,"Netuma thalassina (Rüppell, 1837)",,"Bykov, V.P.0 Marine Fishes: Chemical composition and processing properties. New Delhi: Amerind Publishing Co. Pvt. Ltd. 322 p. (Ref. 4883)",Pascualita Sa-a""")
    val header = new CSVParser().parseLine(headers)
    val firstLine = new CSVParser().parseLine(fourLines.head)
    val aRecord: Map[String, String] = (header.toSeq zip firstLine).toMap

    aRecord.get("Scientific Name") shouldBe Some("Balaenoptera musculus")
    aRecord.get("Value") shouldBe Some("154321.3045")
    aRecord.get("Measurement URI") shouldBe Some("http://purl.obolibrary.org/obo/VT_0001259")
    aRecord.get("Units URI (normalized)") shouldBe Some("http://purl.obolibrary.org/obo/UO_0000009")
  }

  "checklist" should "be filtered using trait filter" in {
    val (checklist: RDD[(String, Int)], traits: RDD[Seq[(String, String)]]) = traitsAndChecklist

    val traitSelectors: Seq[String] = """bodyMass greaterThan 1025 g|bodyMass greaterThan 1 kg""".split( """[\|,]""")

    val checklistMatchingTraits: RDD[(String, Int)] = ChecklistGenerator.filterByTraits(checklist, traits, traitSelectors)

    checklistMatchingTraits.collect().length shouldBe 1
    checklistMatchingTraits.collect() should contain( """bla | boo | Balaena mysticetus""", 23)
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


  def traitsAndChecklist: (RDD[(String, Int)], RDD[Seq[(String, String)]]) = {
    val headers = """EOL page ID,Scientific Name,Common Name,Measurement,Value,Measurement URI,Value URI,Units (normalized),Units URI (normalized),Raw Value (direct from source),Raw Units (direct from source),Raw Units URI (normalized),Supplier,Content Partner Resource URL,source,citation,measurement method,statistical method,life stage,scientific name,measurement remarks,Reference,contributor"""
    val fourLines = Seq( """328574,Balaenoptera musculus,Blue Whale,body mass,154321.3045,http://purl.obolibrary.org/obo/VT_0001259,"",kg,http://purl.obolibrary.org/obo/UO_0000009,154321304.5,g,http://purl.obolibrary.org/obo/UO_0000021,PanTHERIA,http://eol.org/content_partners/652/resources/704,"Data set supplied by Kate E. Jones. The data can also be accessed at Ecological Archives E090-184-D1, <a href=""http://esapubs.org/archive/ecol/E090/184/"">http://esapubs.org/archive/ecol/E090/184/</a>, <a href=""http://esapubs.org/archive/ecol/E090/184/PanTHERIA_1-0_WR05_Aug2008.txt"">http://esapubs.org/archive/ecol/E090/184/PanTHERIA_1-0_WR05_Aug2008.txt</a>","Kate E. Jones, Jon Bielby, Marcel Cardillo, Susanne A. Fritz, Justin O'Dell, C. David L. Orme, Kamran Safi, Wes Sechrest, Elizabeth H. Boakes, Chris Carbone, Christina Connolly, Michael J. Cutts, Janine K. Foster, Richard Grenyer, Michael Habib, Christopher A. Plaster, Samantha A. Price, Elizabeth A. Rigby, Janna Rist, Amber Teacher, Olaf R. P. Bininda-Emonds, John L. Gittleman, Georgina M. Mace, and Andy Purvis. 2009. PanTHERIA: a species-level database of life history, ecology, and geography of extant and recently extinct mammals. Ecology 90:2648.","Mass of adult (or age unspecified) live or freshly-killed specimens (excluding pregnant females) using captive, wild, provisioned, or unspecified populations; male, female, or sex unspecified individuals; primary, secondary, or extrapolated sources; all measures of central tendency; in all localities. Based on information from primary and secondary literature sources. See source for details.",average,adult,Balaenoptera musculus,,,"""
      , """328577,Balaena mysticetus,Bowhead Whale,body mass,79691.17899,http://purl.obolibrary.org/obo/VT_0001259,"",kg,http://purl.obolibrary.org/obo/UO_0000009,79691178.98999999,g,http://purl.obolibrary.org/obo/UO_0000021,PanTHERIA,http://eol.org/content_partners/652/resources/704,"Data set supplied by Kate E. Jones. The data can also be accessed at Ecological Archives E090-184-D1, <a href=""http://esapubs.org/archive/ecol/E090/184/"">http://esapubs.org/archive/ecol/E090/184/</a>, <a href=""http://esapubs.org/archive/ecol/E090/184/PanTHERIA_1-0_WR05_Aug2008.txt"">http://esapubs.org/archive/ecol/E090/184/PanTHERIA_1-0_WR05_Aug2008.txt</a>","Kate E. Jones, Jon Bielby, Marcel Cardillo, Susanne A. Fritz, Justin O'Dell, C. David L. Orme, Kamran Safi, Wes Sechrest, Elizabeth H. Boakes, Chris Carbone, Christina Connolly, Michael J. Cutts, Janine K. Foster, Richard Grenyer, Michael Habib, Christopher A. Plaster, Samantha A. Price, Elizabeth A. Rigby, Janna Rist, Amber Teacher, Olaf R. P. Bininda-Emonds, John L. Gittleman, Georgina M. Mace, and Andy Purvis. 2009. PanTHERIA: a species-level database of life history, ecology, and geography of extant and recently extinct mammals. Ecology 90:2648.","Mass of adult (or age unspecified) live or freshly-killed specimens (excluding pregnant females) using captive, wild, provisioned, or unspecified populations; male, female, or sex unspecified individuals; primary, secondary, or extrapolated sources; all measures of central tendency; in all localities. Based on information from primary and secondary literature sources. See source for details.",average,adult,Balaena mysticetus,,,"""
      , """219907,Pseudopentaceros wheeleri,Boarfish,body mass,"1,200",http://purl.obolibrary.org/obo/VT_0001259,"",
          |g,http://purl.obolibrary.org/obo/UO_0000021,"1,200",g,http://purl.obolibrary.org/obo/UO_0000021,FishBase,
          |http://eol.org/content_partners/2/resources/42,"<a href=""http://www.fishbase.org/summary/SpeciesSummary.
          |php?id=12364"">http://www.fishbase.org/summary/SpeciesSummary.php?id=12364</a>",,,max,,"Pentaceros wheele
          |ri (Hardy, 1983)",,"Fadeev, N.S.0 Guide to biology and fisheries of fishes of the North Pacific Ocean. Vl
          |adivostok, TINRO-Center. 366 p. (Ref. 56527)",Liza Q. Agustin"""
      , """222044,Sargochromis carlottae,Rainbow Happy,body mass,"1,000",http://purl.obolibrary.org/obo/VT_0001259,"",g,http://purl.obolibrary.org/obo/UO_0000021,"1,000",g,http://purl.obolibrary.org/obo/UO_0000021,FishBase,http://eol.org/content_partners/2/resources/42,"<a href=""http://www.fishbase.org/summary/SpeciesSummary.php?id=5364"">http://www.fishbase.org/summary/SpeciesSummary.php?id=5364</a>",,,max,,,,"Skelton, P.H.0 A complete guide to the freshwater fishes of southern Africa. Southern Book Publishers. 388 p. (Ref. 7248)",Susan M. Luna"""
      , """1003713,Netuma thalassina,Giant Catfish,body mass,"1,000",http://purl.obolibrary.org/obo/VT_0001259,"",g,http://purl.obolibrary.org/obo/UO_0000021,"1,000",g,http://purl.obolibrary.org/obo/UO_0000021,FishBase,http://eol.org/content_partners/2/resources/42,"<a href=""http://www.fishbase.org/summary/SpeciesSummary.php?id=10220"">http://www.fishbase.org/summary/SpeciesSummary.php?id=10220</a>",,,max,,"Netuma thalassina (Rüppell, 1837)",,"Bykov, V.P.0 Marine Fishes: Chemical composition and processing properties. New Delhi: Amerind Publishing Co. Pvt. Ltd. 322 p. (Ref. 4883)",Pascualita Sa-a""")
    val header = new CSVParser().parseLine(headers)

    val checklist = sc.parallelize(Seq(( """bla | boo | Balaena mysticetus""", 23), ( """bla | boo | Netuma thalassina""", 11), ( """bla | boo | Mickey mousus""", 12)))
    val traitsRDD = sc.parallelize(fourLines)
    val traits = ChecklistGenerator.readRows(header, traitsRDD)
    (checklist, traits)
  }



  "concatenating rows" should "link the record with the concatenated values" in {
    val headers = Seq("id", "dwc:scientificName", "dwc:scientificNameAuthorship", "dwc:someOther")
    val lines = Seq("123,Mickey mousus,walt,xyz", "345,Donald duckus,walt,zzz")
    val rdd = sc.parallelize(lines)

    val recordLinks = RecordLinker.handleLines(rdd, headers, "id", NameConcat.concatName).collect()

    recordLinks should contain(("345", "Donald duckus walt"))
    recordLinks should contain(("123", "Mickey mousus walt"))
    recordLinks should not contain (("h2", "v2_2"))
    recordLinks should not contain (("h2", "v2_1"))
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
    try {
      CassandraConnector(sc.getConf).withSessionDo { session =>
        session.execute(CassandraUtil.checklistKeySpaceCreate)
        session.execute(CassandraUtil.checklistTableCreate)
        session.execute(CassandraUtil.checklistRegistryTableCreate)
        session.execute(s"TRUNCATE idigbio.checklist")
      }
      val otherLines = Seq(("Mammalia|Insecta", "LINE(1 2 3 4)", "checklist item", 1)
        , ("Mammalia|Insecta", "LINE(1 2 3 4)", "other checklist item", 1))

      val rdd = sc.parallelize(otherLines)
        .saveToCassandra("idigbio", "checklist", CassandraUtil.checklistColumns)

      sc.parallelize(Seq(("bla|bla", "something", "running", 123L)))
        .saveToCassandra("idigbio", "checklist_registry", CassandraUtil.checklistRegistryColumns)
    } catch {
      case e: IOException => {
        fail("failed to connect to cassandra. do you have it running?", e);
      }
    }
  }


}
