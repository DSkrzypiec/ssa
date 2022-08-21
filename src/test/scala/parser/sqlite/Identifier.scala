import org.scalatest.flatspec._
import org.scalatest.matchers._
import fastparse._, NoWhitespace._
import dev.dskrzypiec.parser.sqlite._
import dev.dskrzypiec.parser.sqlite.Identifier._

class IdentifierTests extends UnitSpec {
  "colName" should "be a valid identifier" in {
    assertResult(Parsed.Success("colName", 7)) {
      parse("colName", id(_))
    }
  }
  "columnName" should "be a valid identifier even though it starts with keyword 'column'" in {
    assertResult(Parsed.Success("columnName", 10)) {
      parse("columnName", id(_))
    }
  }
  "_colName" should "be a valid identifier" in {
    assertResult(Parsed.Success("_colName", 8)) {
      parse("_colName", id(_))
    }
  }
  "col_name_8" should "be a valid identifier" in {
    assertResult(Parsed.Success("col_name_8", 10)) {
      parse("col_name_8", id(_))
    }
  }
  "select (keyword)" should "not be a valid identifier" in {
    parse("select", id(_)) shouldBe a [Parsed.Failure]
  }
  "CURRENT_TIMESTAMP (keyword)" should "be not a valid identifier" in {
    parse("CURRENT_TIMESTAMP", id(_)) shouldBe a [Parsed.Failure]
  }
  "col_name_UP$42" should "be a valid identifier" in {
    assertResult(Parsed.Success("col_name_UP$42", 14)) {
      parse("col_name_UP$42", id(_))
    }
  }
  "8_colName" should "not be a valid identifier" in {
    parse("8_colName", id(_)) shouldBe a [Parsed.Failure]
  }
}
