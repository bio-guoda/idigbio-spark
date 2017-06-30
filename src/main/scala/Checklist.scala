case class ChecklistItem(taxonPath: String, recordCount: Long)
case class Checklist(taxonSelector: String, wktString: String, traitSelector: String, uuid: String, itemCount: Long, lastModified: Long)
