import com.datastax.spark.connector.SomeColumns

object CassandraUtil {
  def checklistKeySpaceCreate: String = {
    s"CREATE KEYSPACE IF NOT EXISTS effechecka WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }"
  }

  def checklistTableCreate: String = {
    s"CREATE TABLE IF NOT EXISTS effechecka.checklist (taxonselector TEXT, wktstring TEXT, traitselector TEXT, taxon TEXT, recordcount int, PRIMARY KEY((taxonselector, wktstring, traitselector), recordcount, taxon))"
  }

  def checklistRegistryTableCreate: String = {
    s"CREATE TABLE IF NOT EXISTS effechecka.checklist_registry (taxonselector TEXT, wktstring TEXT, traitselector TEXT, status TEXT, recordcount int, PRIMARY KEY(taxonselector, wktstring, traitselector))"
  }

  def checklistColumns: SomeColumns = {
    SomeColumns("taxonselector", "wktstring", "traitselector", "taxon", "recordcount")
  }

  def checklistRegistryColumns: SomeColumns = {
    SomeColumns("taxonselector", "wktstring", "traitselector", "status", "recordcount")
  }

  def occurrenceCollectionTableCreate: String = {
    s"CREATE TABLE IF NOT EXISTS effechecka.occurrence_collection (taxonselector TEXT, wktstring TEXT, traitselector TEXT, taxon TEXT, lat DOUBLE, lng DOUBLE, start TIMESTAMP, end TIMESTAMP, id TEXT, added TIMESTAMP, source TEXT, PRIMARY KEY((taxonselector, wktstring, traitselector), added, source, id, taxon, start, end, lat, lng))"
  }

  def occurrenceSearchTableCreate: String = {
    s"CREATE TABLE IF NOT EXISTS effechecka.occurrence_search (source TEXT, id TEXT, taxonselector TEXT, wktstring TEXT, traitselector TEXT, PRIMARY KEY((source), id, taxonselector, wktstring, traitselector))"
  }

  def monitorsTableCreate: String = {
    s"CREATE TABLE IF NOT EXISTS effechecka.monitors (taxonselector TEXT, wktstring TEXT, traitselector TEXT, accessed_at TIMESTAMP, PRIMARY KEY(taxonselector, wktstring, traitselector))"
  }

  def occurrenceFirstAddedSearchTableCreate: String = {
    s"CREATE TABLE IF NOT EXISTS effechecka.occurrence_first_added_search (source TEXT, added TIMESTAMP, id TEXT, PRIMARY KEY((source), added, id))"
  }

  def occurrenceCollectionRegistryTableCreate: String = {
    s"CREATE TABLE IF NOT EXISTS effechecka.occurrence_collection_registry (taxonselector TEXT, wktstring TEXT, traitselector TEXT, status TEXT, recordcount int, PRIMARY KEY(taxonselector, wktstring, traitselector))"
  }

  def occurrenceCollectionColumns: SomeColumns = {
    SomeColumns("taxonselector", "wktstring", "traitselector", "taxon", "lat", "lng", "start", "end", "id", "added", "source")
  }

  def occurrenceSearchColumns: SomeColumns = {
    SomeColumns("source", "id", "taxonselector", "wktstring", "traitselector")
  }

  def occurrenceFirstAddedSearchColumns: SomeColumns = {
    SomeColumns("source", "added", "id")
  }

  def occurrenceCollectionRegistryColumns: SomeColumns = {
    SomeColumns("taxonselector", "wktstring", "traitselector", "status", "recordcount")
  }

}
