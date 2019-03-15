package bio.guoda

import java.io.File
import java.net.URI

import org.apache.commons.io.IOUtils
import org.scalatest._

class DwC$Test extends FlatSpec with Matchers {

  "reading a gbif meta.xml" should "returned an ordered list of columns headers" in {
    val metaOpt = DwC.readMeta(getClass.getResource("/gbif/meta.xml").toURI)
    metaOpt match {
      case Some(meta) => {
        meta.delimiter should be("""\t""")
        meta.quote should be(null)
        meta.skipHeaderLines should be(1)
        meta.schema.size should be(224)
        meta.schema.fieldNames should contain("http://rs.tdwg.org/dwc/terms/genus")
        meta.schema.fields.count(_.nullable) should be(meta.schema.size)
        meta.fileURIs.size should be(1)
        meta.fileURIs should contain(getClass.getResource("/gbif/occurrence.txt").toString)
      }
      case None => {
        fail("expected some meta")
      }
    }

  }

  "reading a gbif meta.xml from String" should "returned an ordered list of columns headers" in {
    val metaOpt = DwC.readMeta(new URI("hdfs:///some/path/meta.xml"),
      Some(IOUtils.toString(getClass.getResourceAsStream("/gbif/meta.xml"))))
    metaOpt match {
      case Some(meta) => {
        meta.delimiter should be("""\t""")
        meta.quote should be(null)
        meta.skipHeaderLines should be(1)
        meta.schema.size should be(224)
        meta.schema.fieldNames should contain("http://rs.tdwg.org/dwc/terms/genus")
        meta.fileURIs.size should be(1)
        meta.fileURIs shouldBe List(URI.create("hdfs:///some/path/occurrence.txt").toString)
      }
      case None => {
        fail("expected some meta")
      }
    }

  }

  "reading a idigbio meta.xml" should "returned an ordered list of columns headers" in {
    val metaOpt = DwC.readMeta(getClass.getResource("/idigbio/meta.xml").toURI)
    metaOpt match {
      case Some(meta) => {
        meta.schema.fieldNames should contain("http://rs.tdwg.org/dwc/terms/%20identificationQualifier")
        meta.schema.fieldNames should contain("http://rs.tdwg.org/dwc/terms/identificationQualifier")
        meta.schema.size should be(186)
        meta.schema.fieldNames should contain("undefined0")
      }
      case None => fail("expected some meta")
    }

  }

  "read a non existing meta.xml" should "blow up gracefully" in {
    val meta = DwC.readMeta(null)
    meta should be(None)
  }

  "occurrence reader" should "use full field terms" in {
    val metaOption = DwC.readMeta(getClass.getResource("/gbif/meta.xml").toURI)
    val meta = metaOption.getOrElse(fail("kaboom!"))
    val filepathURI = new URI(meta.fileURIs.head)
    new File(filepathURI).exists should be(true)    
  }

  "occurrence reader inaturalist" should "define a quote character" in {
    val metaOption = DwC.readMeta(getClass.getResource("/inaturalist/meta.xml").toURI)
    val meta = metaOption.getOrElse(fail("kaboom!"))
    val filepathURI = new URI(meta.fileURIs.head)
    new File(filepathURI).exists should be(true)
    meta.quote should be("\"")
  }

}
