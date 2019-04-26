package bio.guoda.preston.spark

import org.scalatest._

import scala.io.Source

// inspired by Zhang et al. 2018. arxiv.org. Scalable Entity Resolution Using Probabilistic Signatures
//on Parallel Databases https://arxiv.org/pdf/1712.09691.pdf

class LinkedRecords$Test extends FlatSpec
  with Matchers
  with ConnectedComponents
  with ProbabilisticSignatures {

  def subRecordsFromUUIDsWords(records: List[String]): Seq[String] = {
    records.mkString(" ").split(nonAlphaNumeric).filter(_.contains("-")).toSeq.distinct
  }

  "connected components" should "be generated from a record from actual records" in {

    val source1 = Source.fromInputStream(getClass.getResourceAsStream("/gbif/occurrence.txt"))
    val source2 = Source.fromInputStream(getClass.getResourceAsStream("/idigbio/occurrence.txt"))
    val recordsAll = (source1.getLines() ++ source2.getLines()).toList
    val a = 1.5
    val b = 0.3


    val some = calculatePairSharedSigProb(recordsAll, a, b, subRecordsFromUUIDsWords)

    val linkedRecords = some.filter(_._2 > 0.9).map(_._1)

    val connectedComponents = towardsSingleParentConnectedComponents(linkedRecords)

    connectedComponents.groupBy(_._1).map(_._2.flatMap(x => List(x._1, x._2)).distinct).foreach(r => {
      r.foreach(println)
      println("---")
    })

  }

}
