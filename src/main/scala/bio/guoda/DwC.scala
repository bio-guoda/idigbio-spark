package bio.guoda

import java.io.StringReader
import java.net.URI

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.xml.{Elem, XML}

object DwC {

  case class Meta(schema: StructType, delimiter: String, quote: String, fileURIs: Seq[String], skipHeaderLines: Int, fileSuffix: String = "")

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

  def asDF(metas: Seq[Meta])(implicit sqlCtx: SQLContext): Seq[(String, DataFrame)] = {
    val metaDFTuples = metas map { meta: Meta =>
      meta.fileURIs map { fileLocation =>
        Console.err.print(s"[$fileLocation] loading...")
        val df = sqlCtx.read.format("csv").
          option("delimiter", meta.delimiter).
          option("quote", meta.quote).
          schema(meta.schema).
          load(fileLocation)
        val exceptHeaders = df.except(df.limit(meta.skipHeaderLines))
        Console.err.println(s" done.")
        (fileLocation, exceptHeaders)
      }
    }
    metaDFTuples.flatten
  }
}
