import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

object SparkUtil {

  @transient private var log_ : Logger = null

  protected def log: Logger = {
    if (log_ == null) {
      log_ = LoggerFactory.getLogger(getClass)
    }
    log_
  }

  def start(conf: SparkConf): SparkContext = {
    console("sparkcontext starting...")
    val context: SparkContext = new SparkContext(conf)
    console("sparkcontext started.")
    context
  }

  def console(msg: String): Unit = {
    log.info(msg)
    println(s"console: [$msg]")
  }

  def stopAndExit(sc: SparkContext): Unit = {
    println("sparkcontext stopping...")
    if (sc == null) {
      console("no spark context found... assuming sparkcontext stopped.")
    } else {
      sc.stop()
      console("sparkcontext stopped.")
    }
    System.exit(0)
  }
}
