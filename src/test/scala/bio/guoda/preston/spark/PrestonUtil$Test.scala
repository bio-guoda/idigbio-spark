package bio.guoda.preston.spark

import java.io.{File, FileInputStream, InputStream}
import java.net.URI
import java.nio.file.{Files, Paths}

import bio.guoda.DwC
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest._

import scala.io.Source
import scala.util.{Success, Try}

trait TestSparkContext extends FlatSpec with Matchers with BeforeAndAfter with SharedSparkContext {

  override val conf = new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.debug.maxToStringFields", "250"). // see https://issues.apache.org/jira/browse/SPARK-15794
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID)
}

class PrestonUtil$Test extends TestSparkContext {

  override implicit def reuseContextIfPossible: Boolean = true

  "output path generate" should "provide a path" in {
    val input = new Path("file:///some/one/two/file")
    val outputPath = new Path("file:///some/output/path")
    val entryName = "somename.txt"
    val output: Path = PrestonUtil.bz2PathForName(entryName, input, outputPath)
    output.toUri should be(URI.create("file:/some/output/path/one/two/file/somename.txt.bz2"))
  }

  "unzip a file" should "unpack and bzip2 compress the entries" in {
    val tuples = testUnzip("/gbif/gbif-test.zip")
    tuples.toList should be(List(("file.zip", Success("meta.xml")), ("file.zip", Success("occurrence.txt"))))
  }

  "unzip a non-zipfile" should "unpack and bzip2 compress the entries" in {
    val tuples = testUnzip("/gbif/meta.xml")
    tuples.toList should be(List())
  }

  "unzip a zip entry" should "unpack and bzip2 compress the entries" in {
    val file: File = createTmpDir

    def testOutputPath(src: String, entryName: String): Path = {
      new Path(new Path(file.toURI), entryName + ".bz2")
    }

    implicit val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    implicit val config: SparkConf = spark.sparkContext.getConf
    val tuples = PrestonUtil.unzip(("file.zip", getClass.getResourceAsStream("/gbif/gbif-test.zip")), PrestonUtil.handleEntry, testOutputPath)

    val expectedMetaFile = new File(file, "meta.xml.bz2")
    val expectedOccurrenceFile = new File(file, "occurrence.txt.bz2")
    tuples.toList should be(List(("file.zip", Success(expectedMetaFile.toURI.toString)),
      ("file.zip", Success(expectedOccurrenceFile.toURI.toString))))

    expectedOccurrenceFile.exists() should be(true)
    val source = Source.fromInputStream(new BZip2CompressorInputStream(new FileInputStream(expectedOccurrenceFile)))
    source.getLines().next() should startWith("gbifID")

    FileUtils.forceDelete(file)

  }

  private def createTmpDir = {
    val file = Files.createTempDirectory(Paths.get("target"), "unzip").toFile
    FileUtils.deleteDirectory(file)
    FileUtils.forceMkdir(file)
    file
  }

  "export a valid zip" should "produce a valid output" in {
    implicit val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val url = getClass.getResource("/valid/d8/5a/d85a59e9011e586fe865e963a9b6465e11a10e26ed85bf36b7707ea0dbfcadc1")

    val prestonDataDir = new File(url.toURI).getParentFile.getParentFile.getParentFile
    prestonDataDir.toString should endWith("valid")

    val tmpDir = createTmpDir

    PrestonUtil.unzip(prestonDataDir.getAbsolutePath, tmpDir.getAbsolutePath)

    val baseOutputDir = new File(URI.create(tmpDir.toURI.toString + "d8/5a/d85a59e9011e586fe865e963a9b6465e11a10e26ed85bf36b7707ea0dbfcadc1"))
    new File(baseOutputDir, "occurrence.txt.bz2").exists() should be(true)
    new File(baseOutputDir, "meta.xml.bz2").exists() should be(true)

    FileUtils.deleteDirectory(tmpDir)
  }

  "generating hash path" should "do the right thing" in {
    val path = PrestonUtil.datasetHashToPath("123455")
    path should be ("12/34/123455")
  }

  "collecting metaData" should "create a sequenceFile" in {
    val meta = getClass.getResource("/unpacked/d8/5a/d85a59e9011e586fe865e963a9b6465e11a10e26ed85bf36b7707ea0dbfcadc1/meta.xml.bz2").toURI
    val paths = Seq(new File(meta).getParentFile.toURI.toString)
    val tmpDir = createTmpDir
    val seqFile = tmpDir.toURI.toString + "/test.seq"

    implicit val context: SparkContext = sc
    val validXml = PrestonUtil.asXmlRDD(paths)
    validXml.count should be(1)

    validXml.saveAsSequenceFile(seqFile)

    new File(URI.create(seqFile + "/part-00000")).exists() should be(true)

    val readValidXml: RDD[(String, String)] = sc.sequenceFile(seqFile)

    readValidXml.equals(validXml)

    readValidXml.count() should be(1)

    val first = readValidXml.first

    first._1 should be("d85a59e9011e586fe865e963a9b6465e11a10e26ed85bf36b7707ea0dbfcadc1")
    first._2 should startWith("""<archive xmlns="http://rs.tdwg.org/dwc/text/">""")

    FileUtils.deleteDirectory(tmpDir)

  }

  "meta objects" should "be generated from meta sequence files" in {
    implicit val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val path = new File(getClass.getResource("/unpacked/meta.xml.seq/part-00000").toURI).getParentFile.getParentFile.toURI.toString
    val tmpDir = createTmpDir
    val metas = PrestonUtil.metaSeqToRDD(path)
    metas.count() should be(1)
    metas.first().fileURIs should be(Seq(getClass.getResource("/unpacked/d8/5a/d85a59e9011e586fe865e963a9b6465e11a10e26ed85bf36b7707ea0dbfcadc1/occurrence.txt.bz2").toURI.toString))
  }

  "parquet files" should "be generated from meta.xml sequences" in {
    implicit val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val path = new File(getClass.getResource("/unpacked/meta.xml.seq/part-00000").toURI).getParentFile.getParentFile.toURI.toString
    val tmpDir = createTmpDir


    val str = tmpDir.getAbsolutePath + "/base"
    val expectedParquetFile = new File(str + "/d8/5a/d85a59e9011e586fe865e963a9b6465e11a10e26ed85bf36b7707ea0dbfcadc1/core.parquet/_SUCCESS")
    expectedParquetFile.exists() should be(false)
    PrestonUtil.writeParquets(path, str)

    expectedParquetFile.exists() should be(true)

    import spark.implicits._
    val derivedFrom = spark.read.parquet(str + "/*/*/*/core.parquet").select("`http://www.w3.org/ns/prov#wasDerivedFrom`").as[String]

    derivedFrom.first() should be("hash://sha256/d85a59e9011e586fe865e963a9b6465e11a10e26ed85bf36b7707ea0dbfcadc1")

  }

  private def testUnzip(resourcePath: String) = {

    def testOutputPath(src: String, entryName: String): Path = {
      new Path(entryName)
    }

    def entryHandler(inputStream: InputStream, dst: Path): Try[String] = {
      Success(dst.toUri.toString)
    }

    implicit val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    PrestonUtil.unzip(("file.zip", getClass.getResourceAsStream(resourcePath)), entryHandler, testOutputPath)
  }


}
