package bio.guoda.preston.spark

import java.io.{File, FileInputStream, InputStream}
import java.net.URI
import java.nio.file.{Files, Paths}

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
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
    val input = new Path("file:///some/path/some/file")
    val outputPath = new Path("file:///some/output/path")
    val entryName = "somename.txt"
    val output: Path = PrestonUtil.outputPathForEntry(entryName, input, outputPath)
    output.toUri should be(URI.create("file:/some/output/path/file/somename.txt.bz2"))
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

    PrestonUtil.export(prestonDataDir.getAbsolutePath, tmpDir.getAbsolutePath)

    new File(new File(tmpDir, "d85a59e9011e586fe865e963a9b6465e11a10e26ed85bf36b7707ea0dbfcadc1"), "occurrence.txt.bz2").exists() should be(true)
    new File(new File(tmpDir, "d85a59e9011e586fe865e963a9b6465e11a10e26ed85bf36b7707ea0dbfcadc1"), "meta.xml.bz2").exists() should be(true)

    FileUtils.deleteDirectory(tmpDir)
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
