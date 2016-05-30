

object OccurrenceSelectors {

  def taxonSelector(config: OccurrenceSelector): OccurrenceExt => Boolean = {
    x => config.taxonSelector.split("\\|").intersect(x.taxonPath.split("\\|")).nonEmpty
  }

  def geoSpatialSelector(config: OccurrenceSelector): OccurrenceExt => Boolean = {
    x => {
      SpatialFilter.parseWkt(config.wktString) match {
        case Some(area) => SpatialFilter.valuesInArea(Seq(x.lat, x.lng), area)
        case _ => false
      }
    }
  }

  def all(config: OccurrenceSelector): OccurrenceExt => Boolean = {
    x => Seq(taxonSelector(config), geoSpatialSelector(config)).forall(_ (x))
  }
}

