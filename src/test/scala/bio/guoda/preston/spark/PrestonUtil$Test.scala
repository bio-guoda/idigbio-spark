package bio.guoda.preston.spark

import java.io.{File, FileInputStream, InputStream}
import java.net.URI
import java.nio.file.{Files, Paths}

import bio.guoda.TestSparkContext
import bio.guoda.preston.spark.PrestonUtil.{metaSeqToSchema, readParquets}
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

import scala.io.Source
import scala.util.{Success, Try}

class PrestonUtil$Test extends TestSparkContext {

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
    implicit val config: Broadcast[Map[String, String]] = spark.sparkContext.broadcast(PrestonUtil.hadoopConfToMap(spark.sparkContext.hadoopConfiguration))
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

 "export a valid zip with root entries" should "produce a valid output" in {
    implicit val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val url = getClass.getResource("/valid/12/02/12027356ed3a72465ccf9f49f2a22be2e78c03876a20d97e81dd221b3459add8")

    val prestonDataDir = new File(url.toURI).getParentFile.getParentFile.getParentFile
    prestonDataDir.toString should endWith("valid")

    val tmpDir = createTmpDir

    PrestonUtil.unzip(prestonDataDir.getAbsolutePath, tmpDir.getAbsolutePath)

    val baseOutputDir = new File(URI.create(tmpDir.toURI.toString + "12/02/12027356ed3a72465ccf9f49f2a22be2e78c03876a20d97e81dd221b3459add8"))
    new File(baseOutputDir, "occurrence.txt.bz2").exists() should be(true)
    new File(baseOutputDir, "eml.xml.bz2").exists() should be(true)
    new File(baseOutputDir, "meta.xml.bz2").exists() should be(true)

    FileUtils.deleteDirectory(tmpDir)
  }

  "generating hash path" should "do the right thing" in {
    val path = PrestonUtil.datasetHashToPath("123455")
    path should be("12/34/123455")
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
    implicit val spark: SparkSession = SparkSession
      .builder()
      .getOrCreate()

    val path = unpackedDir
    val tmpDir = createTmpDir
    val metas = PrestonUtil.metaSeqToRDD(path)
    metas.count() should be(1)
    metas.first().fileURIs should be(Seq(getClass.getResource("/unpacked/d8/5a/d85a59e9011e586fe865e963a9b6465e11a10e26ed85bf36b7707ea0dbfcadc1/occurrence.txt.bz2").toURI.toString))
  }

  "a merged schema" should "be generated from meta sequence files" in {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .getOrCreate()

    val path = unpackedDir
    val schema: StructType = PrestonUtil.metaSeqToSchema(path)
    schema.fields.length should be(186 + 1 + 1)
  }


  "parquet files" should "be generated from meta.xml sequences with case sensitive fields" in {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .getOrCreate()

    val path = unpackedDir
    val tmpDir = createTmpDir


    val parquetBaseDir = tmpDir.getAbsolutePath + "/base"
    val expectedParquetFile = new File(parquetBaseDir + "/d8/5a/d85a59e9011e586fe865e963a9b6465e11a10e26ed85bf36b7707ea0dbfcadc1/core.parquet/_SUCCESS")
    expectedParquetFile.exists() should be(false)
    PrestonUtil.writeParquets(path, parquetBaseDir)

    expectedParquetFile.exists() should be(true)

    val schema = metaSeqToSchema(path)
    val df = readParquets(parquetBaseDir, schema)

    import spark.implicits._
    val derivedFrom = df.select("`http://www.w3.org/ns/prov#wasDerivedFrom`").as[String]

    derivedFrom.first() should be("hash://sha256/d85a59e9011e586fe865e963a9b6465e11a10e26ed85bf36b7707ea0dbfcadc1")

    val queryAttempt = Try(df.select("`http://www.w3.org/ns/prov#wasderivedfrom`").as[String])

    // see https://stackoverflow.com/a/43068079
    queryAttempt shouldBe 'failure

  }

  private def unpackedDir = {
    new File(getClass.getResource("/unpacked/meta.xml.seq/part-00000").toURI).getParentFile.getParentFile.toURI.toString
  }

  "dwc.parquet" should "be ok with missing fields in specified schema" in {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .getOrCreate()

    val schema = StructType(Seq(
      StructField(name = "http://example.org", dataType = DataTypes.StringType, nullable = true),
      StructField(name = "http://rs.tdwg.org/dwc/terms/locality", dataType = DataTypes.StringType, nullable = true),
      StructField(name = "http://rs.tdwg.org/dwc/terms/scientificName", dataType = DataTypes.StringType, nullable = true),
      StructField(name = "http://rs.tdwg.org/dwc/terms/scientificname", dataType = DataTypes.StringType, nullable = true))
    )

    val df = PrestonUtil.readParquets(unpackedDir, schema)
    df.take(1).length should be(1)
  }

  "dwc.parquet" should "be ok with fields that only differ in case" in {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .getOrCreate()

    val schema = StructType(Seq(
      StructField(name = "http://rs.tdwg.org/dwc/terms/scientificName", dataType = DataTypes.StringType, nullable = true),
      StructField(name = "http://rs.tdwg.org/dwc/terms/scientificname", dataType = DataTypes.StringType, nullable = true))
    )

    val df = PrestonUtil.readParquets(unpackedDir, schema)
    df.take(1).length should be(1)
  }

  "json lines" should "be ok with sparse values for nullable schema fields" in {
    val json1 = """{ "key1": "val1", "key2":"val2" }"""
    val json2 = """{ "key1": "val1_1" }"""
    val json3 = """{ "key2": "val2_1", "key1":"val1_2" }"""

    val schema = StructType(Seq(
      StructField(name = "key1", dataType = DataTypes.StringType, nullable = true),
      StructField(name = "key2", dataType = DataTypes.StringType, nullable = true))
    )

    implicit val spark: SparkSession = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val themData = spark.read.schema(schema).json(spark.createDataset(Seq(json1, json2, json3)))

    themData.show(10)

  }

  "csv lines" should "also be ok with sparse values for nullable schema fields" in {
    val json1 = """{ "key1": "val1", "key2":"val2" }"""
    val json2 = """{ "key1": "val1_1" }"""
    val json3 = """{ "key2": "val2_1", "key1":"val1_2" }"""

    val schema = StructType(Seq(
      StructField(name = "key1", dataType = DataTypes.StringType, nullable = true),
      StructField(name = "key2", dataType = DataTypes.StringType, nullable = true))
    )

    implicit val spark: SparkSession = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val themData = spark.read.schema(schema).json(spark.createDataset(Seq(json1, json2, json3)))

    themData.show(10)

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
