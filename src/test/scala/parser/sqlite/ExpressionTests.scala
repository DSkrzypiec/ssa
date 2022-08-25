import org.scalatest.flatspec._
import org.scalatest.matchers._
import fastparse._, NoWhitespace._
import dev.dskrzypiec.parser.sqlite._
import dev.dskrzypiec.parser.sqlite.Expr._

class ExpressionTests extends UnitSpec {
  "42 (with expr parser)" should "be parsed as expression literal" in {
    assertResult(Parsed.Success(SqliteIntegerLit(42), 2)) { parse("42", expr(_)) }
  }
  "column_name (with expr parser)" should "be parsed as expression - column expr" in {
    val expectedObj = SqliteColumnExpr(columnName = "column_name")
    assertResult(Parsed.Success(expectedObj, 11)) {
      parse("column_name", expr(_))
    }
  }
  "11+100*42" should "be parsed as simple binary operation" in {
    val expected = SqliteBinaryOp(
      op = ADD,
      left = SqliteIntegerLit(11),
      right = SqliteBinaryOp(
        op = MUL,
        left = SqliteIntegerLit(100),
        right = SqliteIntegerLit(42)
      )
    )
    assertResult(Parsed.Success(expected, 9)) { parse("11+100*42", expr(_)) }
  }
  "column_name*42" should "be parsed as simple binary operation" in {
    val expected = SqliteBinaryOp(
      op = MUL,
      left = SqliteColumnExpr(columnName = "column_name"),
      right = SqliteIntegerLit(42)
    )
    assertResult(Parsed.Success(expected, 14)) { parse("column_name*42", expr(_)) }
  }
  "(x-10)*(y+42.32)" should "be parsed as simple binary operation" in {
    val expected = SqliteBinaryOp(
      op = MUL,
      left = SqliteBinaryOp(op = SUB, SqliteColumnExpr(columnName = "x"), SqliteIntegerLit(10)),
      right = SqliteBinaryOp(op = ADD, SqliteColumnExpr(columnName = "y"), SqliteDoubleLit(42.32))
    )
    assertResult(Parsed.Success(expected, 16)) { parse("(x-10)*(y+42.32)", expr(_)) }
  }
  "(x - 10) * (y + 42.32)" should "be parsed as simple binary operation including whitespace" in {
    val expected = SqliteBinaryOp(
      op = MUL,
      left = SqliteBinaryOp(op = SUB, SqliteColumnExpr(columnName = "x"), SqliteIntegerLit(10)),
      right = SqliteBinaryOp(op = ADD, SqliteColumnExpr(columnName = "y"), SqliteDoubleLit(42.32))
    )
    assertResult(Parsed.Success(expected, 22)) { parse("(x - 10) * (y + 42.32)", expr(_)) }
  }
  "x || y + z * 10" should "be parsed as simple binary operation" in {
    val expected = SqliteBinaryOp(
      op = ADD,
      left = SqliteBinaryOp(op = CONCAT, SqliteColumnExpr(columnName = "x"), SqliteColumnExpr(columnName = "y")),
      right = SqliteBinaryOp(op = MUL, SqliteColumnExpr(columnName = "z"), SqliteIntegerLit(10))
    )
    assertResult(Parsed.Success(expected, 15)) { parse("x || y + z * 10", expr(_)) }
  }
  "12 + 15    = 27" should "be parsed as simple binary operation" in {
    val expected = SqliteBinaryOp(
      op = EQUAL,
      left = SqliteBinaryOp(ADD, SqliteIntegerLit(12), SqliteIntegerLit(15)),
      right = SqliteIntegerLit(27)
    )
    assertResult(Parsed.Success(expected, 15)) { parse("12 + 15    = 27", expr(_)) }
  }
  "10 + a || b >= 9 % 2" should "be parsed as simple binary operation" in {
    val expected = SqliteBinaryOp(
      op = GREATER_OR_EQ,
      left = SqliteBinaryOp(
        op = ADD,
        left = SqliteIntegerLit(10),
        right = SqliteBinaryOp(CONCAT, SqliteColumnExpr(columnName = "a"), SqliteColumnExpr(columnName = "b"))
      ),
      right = SqliteBinaryOp(MOD, SqliteIntegerLit(9), SqliteIntegerLit(2))
    )
    assertResult(Parsed.Success(expected, 20)) { parse("10 + a || b >= 9 % 2", expr(_)) }
  }
  "case when 1 + 3 > 1 then 10 else 0 end + 10 * 42" should "be parsed with case and bin op precedence" in {
    val expected = SqliteBinaryOp(
      op = ADD,
      left = SqliteCaseExpr(
        whenThens = List(
          SqliteCaseWhenThen(
            when = SqliteBinaryOp(
              op = GREATER_THEN,
              left = SqliteBinaryOp(ADD, SqliteIntegerLit(1), SqliteIntegerLit(3)),
              right = SqliteIntegerLit(1)
            ),
            then = SqliteIntegerLit(10),
          )
        ),
        elseExpr = Some(SqliteIntegerLit(0))
      ),
      right = SqliteBinaryOp(MUL, SqliteIntegerLit(10), SqliteIntegerLit(42))
    )
    assertResult(Parsed.Success(expected, 48)) {
      parse("case when 1 + 3 > 1 then 10 else 0 end + 10 * 42", expr(_))
    }
  }

  // much more unit tests for binary ops
}
