package bio.guoda.preston.spark

case class SignaturePair(rpair: (String, String), sr: String, ps: Double)


trait ProbabilisticSignatures {

  val nonAlphaNumeric = "[^a-zA-Z\\d\\:\\-_\\\\/\\.]"

  def calculatePairSharedSigProb(recordsAll: List[String],
                                 a: Double,
                                 b: Double,
                                 generateSubRecords: List[String] => Seq[String]): Seq[((String, String), Double)] = {

    val subrecords = generateSubRecords(recordsAll)

    val subrecordsTrimmed = excludeSubSubRecords(subrecords)

    val recordSetForSubRecords = subrecordsTrimmed.map(sr => (sr, recordsAll.filter(r => generateSubRecords(List(r)).contains(sr))))

    val tuples = recordSetForSubRecords.flatMap(s => {
      val subrecord = s._1
      pairwiseRecords(s._2)
        .map(pair => SignaturePair(pair, subrecord, probabilitySubrecord(a, b, s._2.length)))
    })

    // removed subrecords that are part of other subrecords


    val byPairs: Iterable[Seq[SignaturePair]] = tuples.groupBy(_.rpair)
      .values


    byPairs
      .map(e => (e.head.rpair, e.foldLeft(1.0)((agg, t) => agg * (1.0 - t.ps))))
      .map(t => (t._1, 1.0 - t._2)).toSeq.sortBy(_._2)

  }


   def excludeSubSubRecords(subrecords: Seq[String]): Seq[String] = {
    val excludedSubrecords = for (
      s1 <- subrecords.zipWithIndex;
      s2 <- subrecords.zipWithIndex
      if s1._2 < s2._2
      if s1._1.split(nonAlphaNumeric).contains(s2._1))
      yield s2._1

    excludedSubrecords.foreach(println)

    subrecords.filterNot(excludedSubrecords.contains(_))
  }

   def pairwiseRecords(records: List[String]): List[(String, String)] = {
    for (
      r1 <- records.zipWithIndex;
      r2 <- records.zipWithIndex
      if r1._2 > r2._2) yield (r1._1, r2._1)
  }

  def probabilitySubrecord(a: Double, b: Double, length: Int): Double = {
    1 / (1 + Math.pow(a, length) * b)
  }



}
