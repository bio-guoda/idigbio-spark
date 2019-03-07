package bio.guoda.preston.spark

import java.io.InputStream
import java.net.URI
import java.util.zip.ZipInputStream

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.util.{Failure, Success, Try}
import scala.xml.Source

object PrestonUtil extends Serializable {

  def outputPathForEntry(name: String, src: Path, dst: Path): Path = {
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
      outputPathForEntry(name, new Path(src), new Path(dst))
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

  // uses hadoop-style path matching: "/home/preston/preston-norway/data/*/*/*"
  def export(src: String, dst: String)(implicit spark: SparkSession): Unit = {
    // many files causes out-of-memory because all file paths are slurped into memory
    // attempt to split file batch in 256 bins
    val pathPatterns = Range(0, 16 * 16)
      .map(x => "%02x".format(x))
      .map(x => s"$src/${x.substring(0, 1)}*/${x.substring(1, 2)}*/*")

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    // spark implementation crashes on pathPatterns with no matches
    // so pre-emptively removing them at expense of extra processing costs
    val nonEmptyPatterns = pathPatterns.filter(x => fs.globStatus(new Path(x)).nonEmpty)
    val unzipAttempts = unzipTo(nonEmptyPatterns, dst)

    unzipAttempts.foreach(x => {
      val errorMsg = x._2 match {
        case Success(_) => ""
        case Failure(exception) => exception.getStackTraceString
      }
      println(s"${x._1}\t${x._2.getOrElse("")}\t${if (x._2.isSuccess) "OK" else "ERROR"}\t$errorMsg")
    })
  }
}
