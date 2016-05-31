import OccurrenceSelectors.OccurrenceFilter

import scala.util.parsing.combinator._

object TraitSelectorParser extends RegexParsers {

  def term: Parser[String] = """[^\s]+""".r ^^ {
    _.toString
  }

  def values: Parser[String] = """[^\s]+""".r ^^ {
    _.toString
  }

  def unit: Parser[String] = """(datetime|string)""".r ^^ {
    _.toString
  }

  def operator: Parser[String] = """(<|>|==|!=)""".r ^^ {
    _.toString
  }

  def selector: Parser[OccurrenceFilter] = term ~ operator ~ values ~ unit ^^ {
    case "eventDate" ~ ">" ~ value ~ "datetime" => {
      (x: Occurrence) => {
        println(s"${x.eventDate} > $value")
        DateUtil.startDate(x.eventDate) > DateUtil.startDate(value)
      }
    }
    case "eventDate" ~ "<" ~ value ~ "datetime" => {
      (x: Occurrence) => {
        println(s"${x.eventDate} < $value")
        DateUtil.endDate(x.eventDate) < DateUtil.endDate(value)
      }
    }
    case "source" ~ "==" ~ value ~ "string" => {
      (x: Occurrence) => x.source.equals(value)
    }
    case "source" ~ "!=" ~ value ~ "string" => {
      (x: Occurrence) => !x.source.equals(value)
    }
    case name ~ op ~ value ~ unit =>
      println(s"unsupported trait selector: $name $op $value $unit")
      OccurrenceSelectors.selectNever
  }

  def config: Parser[OccurrenceFilter] = selector ~ rep("|" ~ selector) ^^ {
    case aSelector ~ list => (aSelector /: list) {
      case (x, "|" ~ y) => (occ: Occurrence) => Seq(x, y).forall(_(occ))
    }
  }
}