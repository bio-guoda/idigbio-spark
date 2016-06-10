import org.joda.time.chrono.ISOChronology
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}
import org.joda.time.{DateTime, DateTimeZone, Interval}

object DateUtil {

  def nonEmpty(someString: String): Boolean = {
    null != someString && someString.nonEmpty
  }

  def validDate(dateString: String): Boolean = {
    nonEmpty(dateString) &&
      (try {
        new DateTime(dateString, DateTimeZone.UTC)
        true
      } catch {
        case e: IllegalArgumentException =>
          try {
            null != Interval.parse(dateString)
          } catch {
            case e: IllegalArgumentException => false
          }
      })
  }

  def startDate(dateString: String): Long = {
    if (dateString.contains("/")) {
      toInterval(dateString).getStartMillis
    } else {
      toUnixTime(dateString)
    }
  }

  def startEndDate(dateString: String): (Long, Long) = {
    if (dateString.contains("/")) {
      val toInterval1: Interval = toInterval(dateString)
      (toInterval1.getStartMillis, toInterval1.getEndMillis)
    } else {
      val time: Long = toUnixTime(dateString)
      (time, time)
    }
  }

  def toInterval(dateString: String): Interval = {
    new Interval(dateString, ISOChronology.getInstance(DateTimeZone.UTC))
  }

  def endDate(dateString: String): Long = {
    if (dateString.contains("/")) {
      toInterval(dateString).getEndMillis
    } else {
      toUnixTime(dateString)
    }
  }

  def toUnixTime(dateString: String): Long = {
    new DateTime(dateString, DateTimeZone.UTC).toDate.getTime
  }

  def basicDateToUnixTime(basicDateString: String): Long = {
    val fmt = ISODateTimeFormat.basicDate()
    fmt.withZoneUTC().parseDateTime(basicDateString).toDate.getTime
  }

  def selectFirstPublished(occ: Occurrence, firstSeen: Occurrence): Occurrence = {
    if (DateUtil.basicDateToUnixTime(occ.sourceDate) < DateUtil.basicDateToUnixTime(firstSeen.sourceDate)) {
      occ
    } else {
      firstSeen
    }
  }
}
