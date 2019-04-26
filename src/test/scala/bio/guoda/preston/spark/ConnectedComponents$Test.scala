package bio.guoda.preston.spark

import org.scalatest._

import scala.io.Source

class ConnectedComponents$Test extends FlatSpec with Matchers with ConnectedComponents {


  "connected components" should "be a tree with one level" in {
    val recordPairs: List[(String, String)] = List(("e1", "ej"), ("e2", "ej"), ("ei", "ej"))
    val expectedRecordPairs = List(("e1", "ej"), ("e1", "e2"), ("e1", "ei"))
    towardsSingleParentConnectedComponents(recordPairs) should be(expectedRecordPairs)
  }

  "connected components" should "be a tree with one level indirect" in {
    val recordPairs: List[(String, String)] = List(("e1", "ej"), ("ej", "e2"), ("ej", "ei"))
    val expectedRecordPairs = List(("e1", "ej"), ("e1", "e2"), ("e1", "ei"))
    towardsSingleParentConnectedComponents(recordPairs) should be(expectedRecordPairs)
  }

  "connected components" should "be two trees with one level indirect" in {
    val recordPairs: List[(String, String)] = List(("e1", "ej"), ("ej", "e2"), ("ej", "ei"), ("ek", "el"), ("em", "el"))
    val expectedRecordPairs = List(("e1", "ej"), ("e1", "e2"), ("e1", "ei"), ("ek", "el"), ("ek", "em"))
    towardsSingleParentConnectedComponents(recordPairs).sorted should be(expectedRecordPairs.sorted)
  }

  "connected components" should "literature example" in {
    val recordPairs: List[(String, String)] = List(("e1", "e2"), ("e1", "e4"), ("e2", "e3"), ("e2", "e4"), ("e2", "e5"), ("e3", "e5"))
    val expectedRecordPairs = List(("e1", "e2"), ("e1", "e3"), ("e1", "e4"), ("e1", "e5"))
    towardsSingleParentConnectedComponents(recordPairs).sorted should be(expectedRecordPairs.sorted)
  }

}
