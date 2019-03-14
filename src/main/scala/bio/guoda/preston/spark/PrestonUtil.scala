package bio.guoda.preston.spark

import java.io.{InputStream, StringReader}
import java.util.zip.ZipInputStream

import bio.guoda.DwC
import bio.guoda.DwC.Meta
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, FileUtil, Path}
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}
import scala.xml.XML

object PrestonUtil extends Serializable {

  def bz2PathForName(name: String, src: Path, dst: Path): Path = {
    val parent = src.getParent
    val grandParent = parent.getParent
    val nestedDst = new Path(new Path(dst, grandParent.getName), parent.getName)
    new Path(new Path(nestedDst, src.getName), s"$name.bz2")
  }

  def unzip(fileAndStream: (String, InputStream),
            entryHandler: (InputStream, Path) => Try[String],
            outputPathFor: (String, String) => Path)
           (implicit spark: SparkSession): Iterator[(String, Try[String])] = {
    Try {
      val is = new ZipInputStream(fileAndStream._2)
      Iterator
        .continually(Try(is.getNextEntry).getOrElse(null))
        .takeWhile({
          entry => {
            if (entry == null && is != null) is.close()
            entry != null
          }
        })
        .map { entry =>
          val path = outputPathFor(fileAndStream._1, entry.getName)
          (fileAndStream._1, entryHandler(is, path))
        }
    } match {
      case Success(value) => value
      case Failure(exception) => Iterator((fileAndStream._1, Failure(exception)))
    }
  }

  def handleEntry(is: InputStream, outputPath: Path)(implicit conf: SparkConf): Try[String] = {
    Try {
      val fs = FileSystem.get(SparkSession.builder().getOrCreate().sparkContext.hadoopConfiguration)

      var dos: FSDataOutputStream = null
      try {
        // overwrites existing by default
        dos = fs.create(outputPath)
        val os = new BZip2CompressorOutputStream(dos)
        val copyAttempt = Try(IOUtils.copy(is, os))
        os.close()
        copyAttempt match {
          case Success(_) => outputPath.toUri.toString
          case Failure(exception) => throw exception
        }
      } finally {
        IOUtils.closeQuietly(dos)
      }
    }
  }

  def saferUnzip(fileAndStream: (String, PortableDataStream),
                 entryHandler: (InputStream, Path) => Try[String],
                 outputPathGen: (String, String) => Path)(implicit spark: SparkSession): Iterator[(String, Try[String])] = {
    val iterator = unzip((fileAndStream._1, fileAndStream._2.open()), entryHandler, outputPathGen)

    new Iterator[(String, Try[String])] {
      override def hasNext: Boolean = Try(iterator.hasNext).getOrElse(false)

      override def next(): (String, Try[String]) = iterator.next()
    }
  }

  def unzipTo(paths: Seq[String], dst: String)(implicit spark: SparkSession): RDD[(String, Try[String])] = {

    val binaryFilesSeq = paths.map(path => spark.sparkContext.binaryFiles(path))

    val binaryFiles = spark.sparkContext.union(binaryFilesSeq)

    def outputPathGenerator(src: String, name: String): Path = {
      bz2PathForName(name, new Path(src), new Path(dst))
    }

    implicit val conf: SparkConf = spark.sparkContext.getConf

    binaryFiles.flatMap(x => {
      Try {
        saferUnzip(x, handleEntry, outputPathGenerator)
      } match {
        case Success(value) => value
        case Failure(exception) => Iterator((x._1, Failure(exception)))
      }
    })
  }

  def partitionedArchivePaths(dataDir: String)(implicit spark: SparkSession): Seq[String] = {
    // many files causes out-of-memory because all file paths are slurped into memory
    // attempt to split file batch in 256 bins
    val pathPatterns = Range(0, 16 * 16)
      .map(x => "%02x".format(x))
      .map(x => s"$dataDir/${x.substring(0, 1)}*/${x.substring(1, 2)}*/*")

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    // spark implementation crashes on pathPatterns with no matches
    // so pre-emptively removing them at expense of extra processing costs
    pathPatterns.filter(x => fs.globStatus(new Path(x)).nonEmpty)
  }

  def asXmlRDD(paths: Seq[String], pathSuffix: String = "/meta.xml.*")(implicit sc: SparkContext): RDD[(String, String)] = {
    val filesSeq = paths.map(path => sc.wholeTextFiles(path + "/meta.xml*"))
    val files = sc.union(filesSeq)

    val parsedAndSerialized: RDD[(String, Try[String])] = files.map(portableFile => {
      (portableFile._1.split("/").reverse.slice(1, 2).head,
        Try {
          scala.xml.XML.load(new java.io.StringReader(portableFile._2)).toString
        })
    })

    parsedAndSerialized
      .filter(_._2.isSuccess)
      .map(x => (x._1, x._2.get))
  }

  // unpack zipfiles to bz2 files to facilitate for parallel processing
  def unzip(src: String, dst: String)(implicit spark: SparkSession): Unit = {
    val nonEmptyPatterns = partitionedArchivePaths(src)
    val unzipAttempts = unzipTo(nonEmptyPatterns, dst)

    unzipAttempts.foreach(x => {
      val errorMsg = x._2 match {
        case Success(_) => ""
        case Failure(exception) => exception.getStackTraceString
      }
      println(s"${x._1}\t${x._2.getOrElse("")}\t${if (x._2.isSuccess) "OK" else "ERROR"}\t$errorMsg")
    })
  }

  // take unpacked darwin core archives and generate sequence files for eml.xml and meta.xml
  // using dataset sha256 hashes as keys.
  def dwcToSeqs(src: String, dest: String)(implicit spark: SparkSession): Unit = {
    val nonEmptyPatterns = partitionedArchivePaths(src)
    implicit val ctx: SparkContext = spark.sparkContext

    Seq("meta.xml", "eml.xml").foreach(suffix => {
      val metaXml = asXmlRDD(nonEmptyPatterns, s"/$suffix*")
      metaXml.saveAsSequenceFile(dest + s"/$suffix.seq")
    })
  }

  def datasetHashToPath(hash: String): String = {
    val chopped = hash.replace("hash://sha256/", "")
    s"${chopped.slice(0, 2)}/${chopped.slice(2, 4)}/$chopped"
  }

  // takes a sequence file with dataset hashes and associated meta.xml and turns it into an RDD
  def metaSeqToRDD(src: String)(implicit spark: SparkSession): RDD[Meta] = {
    val srcShort: String = chopTrailingSlash(src)
    val metaRDD: RDD[(String, String)] = spark.sparkContext.sequenceFile(s"$srcShort/meta.xml.seq")
    val metas: RDD[Meta] = metaRDD
      .map(p => (p._1, XML.load(new StringReader(p._2))))
      .flatMap(p => {
        DwC.parseMeta(p._2) match {
          case Some(meta) =>
            val files = meta.fileURIs.map(file => s"$srcShort/${datasetHashToPath(p._1)}/$file.bz2")
            Some(meta.copy(fileURIs = files, derivedFrom = s"hash://sha256/${p._1}"))
          case None => None
        }
      })
    metas
  }

  def metaSeqToSchema(path: String)(implicit spark: SparkSession): StructType = {
    val metas = PrestonUtil.metaSeqToRDD(path)
    val fields = metas.flatMap(meta => meta.schema.fields).distinct().collect()
    StructType((fields ++ Seq(StructField(name = "http://www.w3.org/ns/prov#wasDerivedFrom", dataType = DataTypes.StringType, nullable = true))).distinct)
  }


  private def chopTrailingSlash(src: String) = {
    if (src.endsWith("/")) src.slice(0, src.length - 1) else src
  }

  def writeParquets(src: String, dst: String)(implicit spark: SparkSession): Unit = {
    val metas = metaSeqToRDD(src)

    for (meta <- metas.toLocalIterator) {
      val maybeSuccess = Try {
        val parquetPath = chopTrailingSlash(dst) + "/" + datasetHashToPath(meta.derivedFrom) + "/core.parquet"
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        if (!fs.exists(new Path(parquetPath + "/_SUCCESS"))) {
          Console.err.print(s"[${meta.fileURIs.mkString(";")}] loading...")
          val df = DwC.toDS(meta, meta.fileURIs, spark)
          df.write
            .parquet(parquetPath)
          Console.err.println(s" done.")
        }
        "OK"
      }
      Console.err.println(s"${meta.derivedFrom}\t${maybeSuccess.getOrElse("ERROR")}")
    }

  }

  def writeJson(src: String, dst: String)(implicit spark: SparkSession): Unit = {
    val metas = metaSeqToRDD(src)

    for (meta <- metas.toLocalIterator) {
      val maybeSuccess = Try {
        val baseDir = chopTrailingSlash(dst) + "/" + datasetHashToPath(meta.derivedFrom)
        val jsonPath = baseDir + "/core-tmp.json"
        Console.err.print(s"[${meta.fileURIs.mkString(";")}] loading...")
        val df = DwC.toDS(meta, meta.fileURIs, spark)
        df.write
          .mode(SaveMode.Overwrite)
          .option("compression", "bzip2")
          .json(jsonPath)

        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        FileUtil.copyMerge(fs, new Path(jsonPath), fs, new Path(baseDir + "/core.json.bz2"), true, spark.sparkContext.hadoopConfiguration, null)
        Console.err.println(s" done.")
        "OK"
      }
      Console.err.println(s"${meta.derivedFrom}\t${maybeSuccess.getOrElse("ERROR")}")
    }

  }

  def readParquets(src: String, schema: StructType)(implicit spark: SparkSession): DataFrame = {
    if (!spark.sparkContext.getConf.getBoolean(key = "spark.sql.caseSensitive", defaultValue = false)) {
      throw new IllegalStateException("please set [spark.sql.caseSensitive=true] to avoid schema merge conflicts")
    }
    spark.read.schema(schema).parquet(s"$src/*/*/*/core.parquet")
  }

  def readJson(src: String, schema: StructType)(implicit spark: SparkSession): DataFrame = {
    if (!spark.sparkContext.getConf.getBoolean(key = "spark.sql.caseSensitive", defaultValue = false)) {
      throw new IllegalStateException("please set [spark.sql.caseSensitive=true] to avoid schema merge conflicts")
    }
    spark.read.schema(schema).json(s"$src/core.json")
  }

  def readMergeAndRewriteParquets(src: String)(implicit spark: SparkSession): Unit = {
    val schema = metaSeqToSchema(src)
    val df = readParquets(src, schema)
    df.write
      .mode(SaveMode.Overwrite)
      .parquet(s"$src/core.parquet")
  }

  def readJsonMergeAndRewriteParquets(src: String)(implicit spark: SparkSession): Unit = {
    val schema = metaSeqToSchema(src)
    val df = readJson(src, schema)
    df.write
      .mode(SaveMode.Overwrite)
      .parquet(s"$src/core.parquet")
  }


}
