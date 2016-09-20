import org.apache.spark.sql.{DataFrame, SQLContext}

object ParquetUtil {
  def readParquet(path: String, sqlContext: SQLContext): DataFrame = {
    // see https://github.com/gimmefreshdata/freshdata/issues/51
      sqlContext.read.option("mergeSchema","true").parquet(path)
    }


}
