import java.util.Date

case class ChecklistItem(taxonPath: String, occurrenceCount: Long)
case class Checklist(taxonSelector: String, wktString: String, traitSelector: String, uuid: String, itemCount: Long, lastModified: Date)
