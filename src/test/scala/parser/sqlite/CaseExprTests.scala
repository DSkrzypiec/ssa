import org.scalatest.flatspec._
import org.scalatest.matchers._
import fastparse._, NoWhitespace._
import dev.dskrzypiec.parser.sqlite._
import dev.dskrzypiec.parser.sqlite.Expr._
import dev.dskrzypiec.parser.sqlite.Expr.CaseExpr._

class CaseExpressionTests extends UnitSpec {
  "case when 1 = 1 then 1 else 0 end" should "be parsed as simple valid CASE" in {
    val expected = SqliteCaseExpr(
      whenThens = List(
        SqliteCaseWhenThen(
          when = SqliteBinaryOp(EQUAL, SqliteIntegerLit(1), SqliteIntegerLit(1)),
          then = SqliteIntegerLit(1)
        )
      ),
      elseExpr = Some(SqliteIntegerLit(0))
    )
    assertResult(Parsed.Success(expected, 33)) { parse("case when 1 = 1 then 1 else 0 end", caseExpr(_)) }
  }
  """
    Case
      when 1 = 1 then 1
      else 0
    end""" should "be parsed as simple valid CASE (including new lines)" in {
    val input = """Case
        WHEN 1 = 1   then 1
        Else    0
      END
    """
    val expected = SqliteCaseExpr(
      whenThens = List(
        SqliteCaseWhenThen(
          when = SqliteBinaryOp(EQUAL, SqliteIntegerLit(1), SqliteIntegerLit(1)),
          then = SqliteIntegerLit(1)
        )
      ),
      elseExpr = Some(SqliteIntegerLit(0))
    )
    assertResult(Parsed.Success(expected, 60)) { parse(input, caseExpr(_)) }
  }
  "CASE WHEN 1 = 1 THEN 1+5 * x ELSE 0 END" should "be parsed as simple valid CASE" in {
    val thenTree = SqliteBinaryOp(
      op = ADD,
      left = SqliteIntegerLit(1),
      right = SqliteBinaryOp(MUL, SqliteIntegerLit(5), SqliteColumnExpr(columnName = "x"))
    )
    val expected = SqliteCaseExpr(
      whenThens = List(
        SqliteCaseWhenThen(
          when = SqliteBinaryOp(EQUAL, SqliteIntegerLit(1), SqliteIntegerLit(1)),
          then = thenTree
        )
      ),
      elseExpr = Some(SqliteIntegerLit(0))
    )
    assertResult(Parsed.Success(expected, 39)) {
      parse("CASE WHEN 1 = 1 THEN 1+5 * x ELSE 0 END", caseExpr(_))
    }
  }
  "CASE WHEN 1 = 1 THEN 1+5 * x ELSE 0 END" should "be parsed with expr(_) as simple valid CASE" in {
    val thenTree = SqliteBinaryOp(
      op = ADD,
      left = SqliteIntegerLit(1),
      right = SqliteBinaryOp(MUL, SqliteIntegerLit(5), SqliteColumnExpr(columnName = "x"))
    )
    val expected = SqliteCaseExpr(
      whenThens = List(
        SqliteCaseWhenThen(
          when = SqliteBinaryOp(EQUAL, SqliteIntegerLit(1), SqliteIntegerLit(1)),
          then = thenTree
        )
      ),
      elseExpr = Some(SqliteIntegerLit(0))
    )
    assertResult(Parsed.Success(expected, 39)) {
      parse("CASE WHEN 1 = 1 THEN 1+5 * x ELSE 0 END", expr(_))
    }
  }
  "case when case when 1 = 1 then 1 else 0 end = 1 then 10 else 42 end" should "be parsed as CASE" in {
    val input = "case when case when 1 = 1 then 1 else 0 end = 1 then 10 else 42 end"
    val innerCase = SqliteCaseExpr(
      whenThens = List(
        SqliteCaseWhenThen(
          when = SqliteBinaryOp(EQUAL, SqliteIntegerLit(1), SqliteIntegerLit(1)),
          then = SqliteIntegerLit(1)
        )
      ),
      elseExpr = Some(SqliteIntegerLit(0))
    )
    val expected = SqliteCaseExpr(
      whenThens = List(
        SqliteCaseWhenThen(
          when = SqliteBinaryOp(
            op = EQUAL,
            left = innerCase,
            right = SqliteIntegerLit(1)
          ),
          then = SqliteIntegerLit(10)
        )
      ),
      elseExpr = Some(SqliteIntegerLit(42))
    )
    assertResult(Parsed.Success(expected, input.length)) { parse(input, caseExpr(_)) }
  }
}
