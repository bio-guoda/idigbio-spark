import org.scalatest._
import java.net.{URI, URL}
import java.io.File

class DwC$Test extends FlatSpec with Matchers {

  "reading a gbif meta.xml" should "returned an ordered list of columns headers" in {
    val metaOpt = DwC.readMeta(getClass.getResource("/gbif/meta.xml"))
    metaOpt match {
      case Some(meta) => {
        meta.delimiter should be("""\t""")
        meta.quote should be(null)
        meta.skipHeaderLines should be(1)
        meta.coreTerms.size should be(224)
        meta.coreTerms should contain("http://rs.tdwg.org/dwc/terms/genus")
        meta.fileURIs.size should be(1)
        meta.fileURIs should contain(getClass.getResource("/gbif/occurrence.txt").toString)
      }
      case None => {
        fail("expected some meta")
      }
    }

  }

  "reading a idigbio meta.xml" should "returned an ordered list of columns headers" in {
    val metaOpt = DwC.readMeta(getClass.getResource("/idigbio/meta.xml"))
    metaOpt match {
      case Some(meta) => {
        meta.coreTerms should contain("http://rs.tdwg.org/dwc/terms/%20identificationQualifier")
        meta.coreTerms should contain("http://rs.tdwg.org/dwc/terms/identificationQualifier")
        meta.coreTerms.size should be(187)
        meta.coreTerms should contain("undefined0")
      }
      case None => {
        fail("expected some meta")
      }
    }

  }

  "read a non existing meta.xml" should "blow up gracefully" in {
    val meta = DwC.readMeta(getClass.getResource("thisdoenstexist"))
    meta should be(None)
  }

  "occurrence reader" should "use full field terms" in {
    val metaOption = DwC.readMeta(getClass.getResource("/gbif/meta.xml"))
    val meta = metaOption.getOrElse(fail("kaboom!"))
    val filepathURI = new URI(meta.fileURIs.head)
    new File(filepathURI).exists should be(true)    
  }

}
