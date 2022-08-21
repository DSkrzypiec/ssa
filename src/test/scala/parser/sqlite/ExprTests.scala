import org.scalatest.flatspec._
import org.scalatest.matchers._
import fastparse._, NoWhitespace._
import dev.dskrzypiec.parser.sqlite._
import dev.dskrzypiec.parser.sqlite.Expr.Literal._
import dev.dskrzypiec.parser.sqlite.Expr.Column._


class ExprColumnTests extends UnitSpec {
  "schema_name.table_name.column_name (with columnFullExpr)" should "be parsed as column expr" in {
    val expected: SqliteExpr = SqliteColumnExpr(
      schemaName = Some("schema_name"),
      tableName = Some("table_name"),
      columnName = "column_name"
    )
    assertResult(Parsed.Success(expected, 34)) {
      parse("schema_name.table_name.column_name", columnFullExpr(_))
    }
  }
  "schema_name.table_name.column_name (with columnExpr)" should "be parsed as column expr" in {
    val expected: SqliteExpr = SqliteColumnExpr(
      schemaName = Some("schema_name"),
      tableName = Some("table_name"),
      columnName = "column_name"
    )
    assertResult(Parsed.Success(expected, 34)) {
      parse("schema_name.table_name.column_name", columnExpr(_))
    }
  }
  "table_name.column_name (with columnSchema)" should "be parsed as column expr" in {
    val expected: SqliteExpr = SqliteColumnExpr(
      tableName = Some("table_name"),
      columnName = "column_name"
    )
    assertResult(Parsed.Success(expected, 22)) {
      parse("table_name.column_name", columnSchema(_))
    }
  }
  "table_name.column_name (with columnExpr)" should "be parsed as column expr" in {
    val expected: SqliteExpr = SqliteColumnExpr(
      tableName = Some("table_name"),
      columnName = "column_name"
    )
    assertResult(Parsed.Success(expected, 22)) {
      parse("table_name.column_name", columnExpr(_))
    }
  }
  "columName (with columnNameOnly)" should "be parsed as column expr" in {
    assertResult(Parsed.Success(SqliteColumnExpr(columnName = "columnName"), 10)) {
      parse("columnName", columnNameOnly(_))
    }
  }
  "columName (with columnExpr)" should "be parsed as column expr" in {
    assertResult(Parsed.Success(SqliteColumnExpr(columnName = "columnName"), 10)) {
      parse("columnName", columnExpr(_))
    }
  }
}

class ExprLiteralTests extends UnitSpec {
  // decimal
  "3.14" should "be parsed as double" in {
    assertResult(Parsed.Success(3.14, 4)) {parse("3.14", decimal(_))}
  }
  ".14" should "be parsed as double" in {
    assertResult(Parsed.Success(0.14, 3)) {parse(".14", decimal(_))}
  }
  "5.19E2 with decimalSci" should "be parsed as double 519.0" in {
    assertResult(Parsed.Success(519.0, 6)) {parse("5.19E2", decimalSci(_))}
  }
  "5.19E2 with decimal" should "be parsed as double 519.0" in {
    assertResult(Parsed.Success(519.0, 6)) {parse("5.19E2", decimal(_))}
  }
  "5.19E+2 with decimal" should "be parsed as double 519.0" in {
    assertResult(Parsed.Success(519.0, 7)) {parse("5.19E+2", decimal(_))}
  }
  "0x123DA with decimalHex" should "be a double 74714.0" in {
    assertResult(Parsed.Success(74714.0, 7)) {parse("0x123DA", decimalHex(_))}
  }
  "0x123DA with decimal" should "be a double 74714.0" in {
    assertResult(Parsed.Success(74714.0, 7)) {parse("0x123DA", decimal(_))}
  }

  // string
  "enclosed 'string'" should "be parsed as SQL string" in {
    assertResult(Parsed.Success("string", 8)) {parse("'string'", string(_))}
  }
  "unenclosed 'string" should "be not be a valid SQL string" in {
    parse("'string", string(_)) shouldBe a [Parsed.Failure]
  }
  "unqouted string" should "be not be a valid SQL string" in {
    parse("string", string(_)) shouldBe a [Parsed.Failure]
  }

  // integer
  "123456" should "be parsed as integer" in {
    assertResult(Parsed.Success(123456, 6)) {parse("123456", integer(_))}
  }
  "123.123" should "not be parsed as integer" in {
    parse("123.123", integer(_)) shouldBe a [Parsed.Failure]
  }
}
