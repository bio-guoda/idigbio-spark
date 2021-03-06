

object TaxonFilter {
  def taxonFields = List("dwc:kingdom", "dwc:phylum", "dwc:class", "dwc:order", "dwc:family", "dwc:genus", "dwc:scientificName")

  def hasTaxa(taxa: List[String], record: Map[String, String]): Boolean = {
    val recordTaxonNames: Seq[String] = taxonFields
      .flatMap(record get)
    hasTaxaInSeq(taxa, recordTaxonNames)
  }

  def hasTaxaInSeq(taxa: Seq[String], recordTaxonNames: Seq[String]): Boolean = {
    if (taxa.isEmpty) {
      true
    } else {
      recordTaxonNames.exists(taxa.contains(_))
    }
  }

}
