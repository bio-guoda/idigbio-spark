import org.apache.spark.SparkContext

object SparkUtil {

  def stopAndExit(sc: SparkContext): Unit = {
    println("sparkcontext stopping...")
    if (sc == null) {
      println("no spark context found... assuming sparkcontext stopped.")
    } else {
      sc.stop()
      println("sparkcontext stopped.")
    }
    System.exit(0)
  }
}
