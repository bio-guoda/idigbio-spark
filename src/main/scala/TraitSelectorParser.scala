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

  def selector: Parser[OccurrenceExt => Boolean] = term ~ operator ~ values ~ unit ^^ {
    case "eventDate" ~ ">" ~ value ~ "datetime" => {
      (x: OccurrenceExt) => x.start > DateUtil.startDate(value)
    }
    case "eventDate" ~ "<" ~ value ~ "datetime" => {
      (x: OccurrenceExt) => x.end < DateUtil.endDate(value)
    }
    case "source" ~ "==" ~ value ~ "string" => {
      (x: OccurrenceExt) => x.psource.equals(value)
    }
    case "source" ~ "!=" ~ value ~ "string" => {
      (x: OccurrenceExt) => !x.psource.equals(value)
    }
    case name ~ op ~ value ~ unit =>
      println(s"unsupported trait selector: $name $op $value $unit")
      (x: OccurrenceExt) => false

  }

  def config: Parser[OccurrenceExt => Boolean] = selector ~ rep("|" ~ selector) ^^ {
    case aSelector ~ list => (aSelector /: list) {
      case (x, "|" ~ y) => (occ: OccurrenceExt) => Seq(x, y).forall(_(occ))
    }
  }
}