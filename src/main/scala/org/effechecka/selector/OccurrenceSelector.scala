package org.effechecka.selector


case class OccurrenceSelector(taxonSelector: String = "", wktString: String = "", traitSelector: String = "", uuid: Option[String] = None, ttlSeconds: Option[Int] = None)
