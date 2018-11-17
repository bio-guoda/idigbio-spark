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
    logInfo("sparkcontext starting...")
    val context: SparkContext = new SparkContext(conf)
    logInfo("sparkcontext started.")
    context
  }

  def logInfo(msg: String): Unit = {
    log.info(msg)
  }

  def stopAndExit(sc: SparkContext): Unit = {
    stopAndExit(sc, 0)
  }

  def stopAndExit(sc: SparkContext, returnCode: Int): Unit = {
    logInfo("sparkcontext stopping...")
    if (sc == null) {
      logInfo("no spark context found... assuming sparkcontext stopped.")
    } else {
      sc.stop()
      logInfo("sparkcontext stopped.")
    }
    System.exit(returnCode)
  }
}
