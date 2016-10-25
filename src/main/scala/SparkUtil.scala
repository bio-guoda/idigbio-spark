import org.apache.spark.SparkContext

object SparkUtil {

  def stopAndExit(sc: SparkContext): Unit = {
    println("sparkcontext stopping...")
    sc.stop()
    println("sparkcontext stopped.")
    System.exit(0)
  }
}
