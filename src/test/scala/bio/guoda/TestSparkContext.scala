package bio.guoda

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

trait TestSparkContext extends FlatSpec with Matchers with BeforeAndAfter with SharedSparkContext {

  def sparkSession: SparkSession = new SQLContext(sc).sparkSession

  override implicit def reuseContextIfPossible: Boolean = true

  override val conf: SparkConf = new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.debug.maxToStringFields", "250"). // see https://issues.apache.org/jira/browse/SPARK-15794
    set("spark.ui.enabled", "false").
    set("spark.sql.caseSensitive", "true").
    set("spark.app.id", appID)
}
