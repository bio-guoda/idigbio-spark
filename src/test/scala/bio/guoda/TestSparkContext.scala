package bio.guoda

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.SparkConf
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

trait TestSparkContext extends FlatSpec with Matchers with BeforeAndAfter with SharedSparkContext {

  override implicit def reuseContextIfPossible: Boolean = true

  override val conf: SparkConf = new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.debug.maxToStringFields", "250"). // see https://issues.apache.org/jira/browse/SPARK-15794
    set("spark.ui.enabled", "false").
    set("spark.sql.caseSensitive", "true").
    set("spark.app.id", appID)


}
