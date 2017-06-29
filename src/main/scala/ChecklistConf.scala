
case class ChecklistConf(occurrenceFiles: Seq[String] = Seq()
                         , traitFiles: Seq[String] = Seq()
                         , traitSelector: Seq[String] = Seq()
                         , taxonSelector: Seq[String] = Seq()
                         , geoSpatialSelector: String = ""
                         , outputFormat: String = "cassandra"
                         , outputPath: String = "hdfs:///guoda/data/monitors"
                         , applyAllSelectors: Boolean = false
                         , firstSeenOnly: Boolean = true)
