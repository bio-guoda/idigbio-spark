import org.apache.spark.sql.{DataFrame, SQLContext}

object ParquetUtil {
  def readParquet(path: String, sqlContext: SQLContext): DataFrame = {
    // see https://github.com/gimmefreshdata/freshdata/issues/51
    sqlContext.read.format("parquet").load(path)
  }
}
