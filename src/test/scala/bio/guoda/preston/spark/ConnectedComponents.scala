package bio.guoda.preston.spark

trait ConnectedComponents {

  def towardsSingleParentConnectedComponents(pairs: Seq[(String, String)]): Seq[(String, String)] = {
    // first element always smaller than second
    val recordPairs = pairs
      .map(p => if (p._1 > p._2) p.swap else p)

    val reducedTreeHeight = for (
      a <- recordPairs.zipWithIndex;
      b <- recordPairs.zipWithIndex;
      if a._2 < b._2)
      yield if (a._1._2 == b._1._1) (List(b._1), List((a._1._1, b._1._2))) else (List(), List())

    val intermediateRemoved = recordPairs.filterNot(p => reducedTreeHeight.flatMap(_._1).contains(p)) ++ reducedTreeHeight.flatMap(_._2)

    val groupedRecordPairs = intermediateRemoved.groupBy(_._2)

    // check for single parent
    if (groupedRecordPairs.exists(_._2.length > 1)) {

      val groupedOrdered = groupedRecordPairs.values
        .flatMap(p => {
          val nodes = p.flatMap(q => List(q._1, q._2))
          val minNode = nodes.min
          nodes.map(n => (minNode, n))
        })
        .filterNot(p => p._1 == p._2) // remove self-loops
        .toSeq
        .distinct


      towardsSingleParentConnectedComponents(groupedOrdered)
    } else pairs
  }


}
