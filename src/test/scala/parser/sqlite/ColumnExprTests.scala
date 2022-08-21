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
