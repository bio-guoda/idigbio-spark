import org.effechecka.selector.OccurrenceSelector

object OccurrenceSelectors {

  type OccurrenceFilter = (Occurrence) => Boolean

  def taxonSelector(config: OccurrenceSelector): OccurrenceFilter = {
    val selectedTaxa: Array[String] = config.taxonSelector.toLowerCase.split("\\|").filter(_.nonEmpty)
    if (selectedTaxa.length == 0) {
      selectAlways
    } else {
      x => {
        val names = x.taxonPath.toLowerCase.split("""\|""")
        // see https://en.wikipedia.org/wiki/N-gram
        val unigram = names.flatMap(_.split("""\s"""))
        val bigram = names.flatMap(_.split("""\s""").sliding(2).map(_.mkString(" ")))
        selectedTaxa.intersect(names ++ unigram ++ bigram).nonEmpty
      }
    }
  }

  def traitSelector(config: OccurrenceSelector): OccurrenceFilter = {
    if (config.traitSelector.trim.isEmpty) {
      selectAlways
    } else {
      TraitSelectorParser.parse(TraitSelectorParser.config, config.traitSelector) match {
        case TraitSelectorParser.Success(selector, _) => selector
        case failure: TraitSelectorParser.NoSuccess => {
          selectNever
        }
      }
    }
  }

  val selectAlways: OccurrenceFilter = {
    (x: Occurrence) => true
  }

  val selectNever: OccurrenceFilter = {
    (x: Occurrence) => false
  }

  def geoSpatialSelector(config: OccurrenceSelector): OccurrenceFilter = {
    SpatialFilter.parseWkt(config.wktString) match {
      case Some(area) =>
        (x: Occurrence) => SpatialFilter.valuesInArea(Seq(x.lat, x.lng), area)
      case _ =>
        selectNever
    }
  }

  def apply(config: OccurrenceSelector): OccurrenceFilter = {
    x => Seq(traitSelector _, taxonSelector _, geoSpatialSelector _)
      .forall(_ (config)(x))
  }

  def toOccurrenceSelector(config: ChecklistConf): OccurrenceSelector = {
    val wktString = config.geoSpatialSelector.trim
    val taxonSelector = config.taxonSelector
    val taxonSelectorString: String = taxonSelector.mkString("|")

    val traitSelectors = config.traitSelector
    val traitSelectorString: String = traitSelectors.mkString("|")

    OccurrenceSelector(taxonSelectorString, wktString, traitSelectorString)
  }

}

