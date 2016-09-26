import scala.collection.mutable


object OccurrenceSelectors {

  type OccurrenceFilter = (Occurrence) => Boolean

  def taxonSelector(config: OccurrenceSelector): OccurrenceFilter = {
    val selectedTaxa: Array[String] = config.taxonSelector.toLowerCase.split("\\|")
    if (selectedTaxa.length > 0) {
      x => {
        val names = x.taxonPath.toLowerCase.split("""\|""")
        // see https://en.wikipedia.org/wiki/N-gram
        val unigram = names.flatMap(_.split("""\s"""))
        val bigram = names.flatMap(_.split("""\s""").sliding(2).map(_.mkString(" ")))
        selectedTaxa.intersect(names ++ unigram ++ bigram).nonEmpty
      }
    } else {
      x => false
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
        x => false
    }
  }

  def apply(config: OccurrenceSelector): OccurrenceFilter = {
    x => Seq(traitSelector _, taxonSelector _, geoSpatialSelector _)
      .forall(_ (config)(x))
  }
}

