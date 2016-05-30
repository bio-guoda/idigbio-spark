import au.com.bytecode.opencsv.CSVParser
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import scopt._
import java.net.URL
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import DwC.Meta

import scala.IllegalArgumentException

trait LinkIdentifiers {

  def toLinkDF(sqlContext: SQLContext, occurrenceDF: DataFrame, columnNames: List[String]): DataFrame = {
    def escapeColumnName(name: String): String = {
      Seq("`", name, "`").mkString("")
    }

    val externalIdColumns = occurrenceDF.schema.
      filter(_.dataType == org.apache.spark.sql.types.StringType).
      map(_.name).
      filter(columnNames.contains(_)).
      map(escapeColumnName)

    val idsOnly = occurrenceDF.select(externalIdColumns.head, externalIdColumns.tail: _*)
    val links = idsOnly.
      flatMap(row => (2 to row.length).toSeq.map(index => Row(row.getString(0), "refers", row.getString(index - 1)))).
      filter(row => {
        val endId = row.getString(2)
        endId != null && endId.nonEmpty
      })


    val linkSchema =
      StructType(
        Seq("start_id", "link_rel", "end_id").map(fieldName => StructField(fieldName, StringType)))

    sqlContext.createDataFrame(links, linkSchema)
  }
}


