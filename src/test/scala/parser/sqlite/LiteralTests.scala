import org.scalatest.flatspec._
import org.scalatest.matchers._
import fastparse._, NoWhitespace._
import dev.dskrzypiec.parser.sqlite._
import dev.dskrzypiec.parser.sqlite.Expr.Literal._

class LiteralTests extends UnitSpec {
  // decimal
  "3.14 (with top literal parser)" should "be parsed as double" in {
    assertResult(Parsed.Success(SqliteDoubleLit(3.14), 4)) {parse("3.14", literal(_))}
  }
  ".14 (with top literal parser)" should "be parsed as double" in {
    assertResult(Parsed.Success(SqliteDoubleLit(0.14), 3)) {parse(".14", literal(_))}
  }
  "5.19E2 with decimalSci (with top literal parser)" should "be parsed as double 519.0" in {
    assertResult(Parsed.Success(SqliteDoubleLit(519.0), 6)) {parse("5.19E2", literal(_))}
  }
  "5.19E2 with decimal (with top literal parser)" should "be parsed as double 519.0" in {
    assertResult(Parsed.Success(SqliteDoubleLit(519.0), 6)) {parse("5.19E2", literal(_))}
  }
  "5.19E+2 with decimal (with top literal parser)" should "be parsed as double 519.0" in {
    assertResult(Parsed.Success(SqliteDoubleLit(519.0), 7)) {parse("5.19E+2", literal(_))}
  }
  "0x123DA with decimalHex (with top literal parser)" should "be a int 74714" in {
    assertResult(Parsed.Success(SqliteHexLit("0x123DA", 74714), 7)) {parse("0x123DA", literal(_))}
  }

  // string
  "enclosed 'string' (with top literal parser)" should "be parsed as SQL string" in {
    assertResult(Parsed.Success(SqliteStringLit("string"), 8)) {parse("'string'", literal(_))}
  }
  "unenclosed 'string (with top literal parser)" should "be not be a valid SQL string" in {
    parse("'string", string(_)) shouldBe a [Parsed.Failure]
  }
  "unqouted string (with top literal parser)" should "be not be a valid SQL string" in {
    parse("string", string(_)) shouldBe a [Parsed.Failure]
  }

  // integer
  "123456 (with top literal parser)" should "be parsed as integer" in {
    assertResult(Parsed.Success(SqliteIntegerLit(123456), 6)) {parse("123456", literal(_))}
  }
  "123.123" should "not be parsed as integer" in {
    parse("123.123", integer(_)) shouldBe a [Parsed.Failure]
  }

  // Trivial literals
  "Null" should "be parsed as null literal" in {
    assertResult(Parsed.Success(SqliteNull(), 4)) { parse("Null", literal(_)) }
  }
  "Nullek" should "be parsed as identifier and NOT literal" in {
    parse("Nullek", literal(_)) shouldBe a [Parsed.Failure]
  }
  "TRUE" should "be parsed as TRUE literal" in {
    assertResult(Parsed.Success(SqliteTrue(), 4)) { parse("TRUE", literal(_)) }
  }
  "TRUEschool" should "be parsed as identifier and NOT literal" in {
    parse("TRUEschool", literal(_)) shouldBe a [Parsed.Failure]
  }
  "false" should "be parsed as FALSE literal" in {
    assertResult(Parsed.Success(SqliteFalse(), 5)) { parse("false", literal(_)) }
  }
  "falsely" should "be parsed as identifier and NOT literal" in {
    parse("falsely", literal(_)) shouldBe a [Parsed.Failure]
  }
  "CURRENT_Date" should "be parsed as CURRENT_DATE literal" in {
    assertResult(Parsed.Success(SqliteCurrentDate(), 12)) { parse("CURRENT_Date", literal(_)) }
  }
  "current_date_var" should "be parsed as identifier and NOT literal" in {
    parse("current_date_var", literal(_)) shouldBe a [Parsed.Failure]
  }
  "current_time" should "be parsed as CURRENT_TIME literal" in {
    assertResult(Parsed.Success(SqliteCurrentTime(), 12)) { parse("current_time", literal(_)) }
  }
  "current_TimeStamp" should "be parsed as CURRENT_TIMESTAMP literal" in {
    assertResult(Parsed.Success(SqliteCurrentTimestamp(), 17)) { parse("current_TimeStamp", literal(_)) }
  }
  "current_timestamper" should "be parsed as identifier and NOT literal" in {
    parse("current_timestamper", literal(_)) shouldBe a [Parsed.Failure]
  }
}

class LiteralSpecTests extends UnitSpec {
  // decimal
  "3.14" should "be parsed as double" in {
    assertResult(Parsed.Success(SqliteDoubleLit(3.14), 4)) {parse("3.14", decimalSimple(_))}
  }
  ".14" should "be parsed as double" in {
    assertResult(Parsed.Success(SqliteDoubleLit(0.14), 3)) {parse(".14", decimalFromDot(_))}
  }
  "5.19E2 with decimalSci" should "be parsed as double 519.0" in {
    assertResult(Parsed.Success(SqliteDoubleLit(519.0), 6)) {parse("5.19E2", decimalSci(_))}
  }
  "5.19E+2 with decimal" should "be parsed as double 519.0" in {
    assertResult(Parsed.Success(SqliteDoubleLit(519.0), 7)) {parse("5.19E+2", decimalSci(_))}
  }
  "0x123DA with decimalHex" should "be a int 74714" in {
    assertResult(Parsed.Success(SqliteHexLit("0x123DA", 74714), 7)) {parse("0x123DA", hex(_))}
  }
  "crap with decimalSimple" should "not be parsed and should not throw exception" in {
    parse("crap", decimalSimple(_)) shouldBe a [Parsed.Failure]
  }

  // string
  "enclosed 'string'" should "be parsed as SQL string" in {
    assertResult(Parsed.Success(SqliteStringLit("string"), 8)) {parse("'string'", string(_))}
  }
  "unenclosed 'string" should "be not be a valid SQL string" in {
    parse("'string", string(_)) shouldBe a [Parsed.Failure]
  }
  "unqouted string" should "be not be a valid SQL string" in {
    parse("string", string(_)) shouldBe a [Parsed.Failure]
  }

  // integer
  "123456" should "be parsed as integer" in {
    assertResult(Parsed.Success(SqliteIntegerLit(123456), 6)) {parse("123456", integer(_))}
  }
  "123.123" should "not be parsed as integer" in {
    parse("123.123", integer(_)) shouldBe a [Parsed.Failure]
  }

  // Error
  "literalErr" should "always fail" in {
    parse("10", literalErr(_)) shouldBe a [Parsed.Failure]
  }
}
