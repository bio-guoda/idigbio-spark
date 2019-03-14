package bio.guoda

import java.io.StringReader
import java.net.URI

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.xml.{Elem, XML}

object DwC {

  case class Meta(schema: StructType, delimiter: String, quote: String, fileURIs: Seq[String], skipHeaderLines: Int, fileSuffix: String = "", derivedFrom: String = "")

  def parseMeta(meta: Elem): Option[Meta] = {

    val delimiter = (meta \\ "core" \\ "@fieldsTerminatedBy") map {
      _ text
    } headOption match {
      case Some(d) => d
      case None => ","
    }
    val quote = if (delimiter == """\t""") null else "\""
    val skipHeaderLines = (meta \\ "core" \\ "@ignoreHeaderLines") map {
      _ text
    } headOption match {
      case Some(d) => Integer.parseInt(d)
      case None => 0
    }
    val fieldTerms = (meta \\ "core" \\ "field" \\ "@term") map {
      _.text.replace(" ", "%20")
    }
    val fieldIds = (meta \\ "core" \\ "field" \\ "@index").map(_.text).map { index: String => Integer.parseInt(index) }
    val termMap = (fieldIds zip fieldTerms).toMap

    // fields index start counting at 0, see http://rs.tdwg.org/dwc/terms/guides/text/#fieldTag
    val paddedFieldTerms = Range(0, 1 + fieldIds.max).map(i => termMap.getOrElse(i, s"undefined$i"))

    val locations = (meta \\ "core" \\ "location").map(_.text)

    val schema = StructType(paddedFieldTerms map {
      StructField(_, StringType)
    })
    Some(Meta(schema, delimiter, quote, locations, skipHeaderLines))
  }


  def readMeta(metaURL: URI, metaString: Option[String] = None): Option[Meta] = try {
    val meta = metaString match {
      case Some(xmlString) => XML.load(new StringReader(xmlString))
      case None => XML.load(metaURL.toURL)
    }

    parseMeta(meta) match {
      case Some(m) => {
        val baseURLParts = metaURL.toString.split("/").reverse.tail.reverse
        val locations = m.fileURIs.map(location => (baseURLParts ++ List(location)) mkString "/")
        val suffixedLocation = locations.map(x => if (metaURL.toString.endsWith(".bz2")) x + ".bz2" else x)
        Some(m.copy(fileURIs = suffixedLocation))
      }
      case None => None
    }
  } catch {
    case e: Exception =>
      println(s"failed to read from [$metaURL]")
      e.printStackTrace()
      None
  }

  def asDF(metas: Seq[Meta])(implicit sqlCtx: SparkSession): Seq[(String, DataFrame)] = {
    val metaDFTuples = metas map { meta: Meta =>
      mapMeta(meta)
    }
    metaDFTuples.flatten
  }

  def mapMeta(meta: Meta)(implicit sqlCtx: SparkSession): Seq[(String, Dataset[Row])] = {
    meta.fileURIs map { fileLocation =>
      (fileLocation, toDS(meta, Seq(fileLocation), sqlCtx))
    }
  }

  def toDS(meta: Meta, files: Seq[String], session: SparkSession): DataFrame = {
    import org.apache.spark.sql.functions.lit
    val df = session.read
      .option("delimiter", meta.delimiter)
      .option("quote", meta.quote)
      .option("spark.sql.caseSensitive", "true")
      .option("header", if (meta.skipHeaderLines > 0) "true" else "false")
      .schema(meta.schema)
      .csv(files: _*)
    df.withColumn("http://www.w3.org/ns/prov#wasDerivedFrom", lit(meta.derivedFrom))
  }

  def readCore(meta: Meta, spark: SparkSession): DataFrame = {
    import org.apache.spark.sql.functions.lit
    val df = spark.read
      .option("delimiter", meta.delimiter)
      .option("quote", meta.quote)
      .schema(meta.schema)
      .csv(meta.fileURIs: _*)
    df.except(df.limit(meta.skipHeaderLines)).withColumn("http://www.w3.org/ns/prov#wasDerivedFrom", lit(meta.derivedFrom))
  }
}
