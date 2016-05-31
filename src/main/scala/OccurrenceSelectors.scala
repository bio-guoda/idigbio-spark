

object OccurrenceSelectors {

  def taxonSelector(config: OccurrenceSelector): OccurrenceExt => Boolean = {
    val selectedTaxa: Array[String] = config.taxonSelector.split("\\|")
    if (selectedTaxa.length > 0) {
      x => selectedTaxa.intersect(x.taxonPath.split("\\|")).nonEmpty
    } else {
      x => false
    }
  }

  def traitSelector(config: OccurrenceSelector): OccurrenceExt => Boolean = {
    if (config.traitSelector.trim.isEmpty) {
      selectAlways
    } else {
      TraitSelectorParser.parse(TraitSelectorParser.config, config.traitSelector) match {
        case TraitSelectorParser.Success(selector, _) => selector
        case failure: TraitSelectorParser.NoSuccess => {
          scala.sys.error(failure.msg)
          selectNever
        }
      }
    }
  }


  val selectAlways: (OccurrenceExt) => Boolean = {
    (x: OccurrenceExt) => true
  }

  val selectNever: (OccurrenceExt) => Boolean = {
    (x: OccurrenceExt) => false
  }

  def geoSpatialSelector(config: OccurrenceSelector): OccurrenceExt => Boolean = {
    SpatialFilter.parseWkt(config.wktString) match {
      case Some(area) => {
        (x: OccurrenceExt) => SpatialFilter.valuesInArea(Seq(x.lat, x.lng), area)
      }
      case _ => {
        x => false
      }
    }
  }

  def all(config: OccurrenceSelector): OccurrenceExt => Boolean = {
    x => Seq(traitSelector _, taxonSelector _, geoSpatialSelector _)
      .forall(_ (config)(x))
  }
}

