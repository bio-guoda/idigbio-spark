import au.com.bytecode.opencsv.CSVParser
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RecordLinker {

  def parseLine(lineString: String): Option[Array[String]] = {
    try {
      val parser: CSVParser = new CSVParser()
      Some(parser.parseLine(lineString))
    } catch {
      case e: Exception =>
        None
    }
  }


}
