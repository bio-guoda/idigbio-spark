import java.io.StringReader
import java.net.URI

import scala.xml.XML

object DwC {

  case class Meta(coreTerms: Seq[String], delimiter: String, quote: String, fileURIs: Seq[String], skipHeaderLines: Int)

  def readMeta(metaURL: URI, metaString: Option[String] = None): Option[Meta] = try {
    val meta = metaString match {
      case Some(xmlString) => XML.load(new StringReader(xmlString))
      case None => XML.load(metaURL.toURL)
    }

    val delimiter = (meta \\ "core" \\ "@fieldsTerminatedBy") map { _ text } headOption match {
      case Some(d) => d
      case None    => ","
    }
    val quote = if (delimiter == """\t""") null else "\""
    val skipHeaderLines = (meta \\ "core" \\ "@ignoreHeaderLines") map { _ text } headOption match {
      case Some(d) => Integer.parseInt(d)
      case None    => 0
    }
    val fieldTerms = (meta \\ "core" \\ "field" \\ "@term") map { _.text.replace(" ", "%20") }
    val fieldIds = (meta \\ "core" \\ "field" \\ "@index").map(_.text).map{(index: String) => Integer.parseInt(index)}
    val termMap = (fieldIds zip fieldTerms).toMap

    // fields index start counting at 0, see http://rs.tdwg.org/dwc/terms/guides/text/#fieldTag
    val paddedFieldTerms = Range(0, 1 + fieldIds.max).map(i => termMap.getOrElse(i, s"undefined$i"))

    val baseURLParts = metaURL.toString.split("/").reverse.tail.reverse
    val locations = (meta \\ "core" \\ "location") map {
      location => (baseURLParts ++ List(location text)) mkString ("/")
    }
    Some(Meta(paddedFieldTerms, delimiter, quote, locations, skipHeaderLines))
  } catch {
    case e: Exception =>
      println(s"failed to read from [$metaURL]")
      None
  }
}
