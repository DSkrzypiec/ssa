import org.scalatest.flatspec._
import org.scalatest.matchers._
import fastparse._, NoWhitespace._
import dev.dskrzypiec.parser.sqlite._
import dev.dskrzypiec.parser.sqlite.Expr.Literal._
import dev.dskrzypiec.parser.sqlite.SelectStmt._


class SelectResultColTests extends UnitSpec {
  "*" should "be parsed as column result start" in {
    val expected = SqliteResultCol(isStar = true)
    assertResult(Parsed.Success(expected, 1)) { parse("*", ResultCol.resultCol(_)) }
  }
  "tableName.*" should "be parsed as column result table name start" in {
    val expected = SqliteResultCol(isStarInTableName = Some("tableName"))
    assertResult(Parsed.Success(expected, 11)) { parse("tableName.*", ResultCol.resultCol(_)) }
  }
  "x + 10 AS Crap" should "be parsed as column result with alias" in {
    val expr = SqliteBinaryOp(ADD, SqliteColumnExpr(columnName = "x"), SqliteIntegerLit(10))
    val expected = SqliteResultCol(colExpr = Some(expr), colAlias = Some("Crap"))
    assertResult(Parsed.Success(expected, 14)) { parse("x + 10 AS Crap", ResultCol.resultCol(_)) }
  }
  "x + 10    Crap" should "be parsed as column result with alias but without AS" in {
    val expr = SqliteBinaryOp(ADD, SqliteColumnExpr(columnName = "x"), SqliteIntegerLit(10))
    val expected = SqliteResultCol(colExpr = Some(expr), colAlias = Some("Crap"))
    assertResult(Parsed.Success(expected, 14)) { parse("x + 10    Crap", ResultCol.resultCol(_)) }
  }
  "AS NewColName" should "be not parsed due to lack of expression" in {
    parse("AS NewColName", ResultCol.resultCol(_)) shouldBe a [Parsed.Failure]
  }
  """x AS newX,
      1 + 10 AS y,
      42 z
  """ should "be parsed as 3 result columns" in {
    val input = """x AS newX,
      1 + 10    y,
      '42'
  """
    val expCol1 = SqliteResultCol(colExpr = Some(SqliteColumnExpr(columnName = "x")), colAlias = Some("newX"))
    val expCol2 = SqliteResultCol(
      colExpr = Some(SqliteBinaryOp(ADD, SqliteIntegerLit(1), SqliteIntegerLit(10))),
      colAlias = Some("y")
    )
    val expCol3 = SqliteResultCol(colExpr = Some(SqliteStringLit("42")))
    val expected = Seq(expCol1, expCol2, expCol3)

    assertResult(Parsed.Success(expected, 43)) { parse(input, ResultCol.resultCols(_)) }
  }
}

class SelectFromSourcesTests extends UnitSpec {
  "schema.tableName AS t" should "be parsed as valid table name with alias" in {
    val expected = SqliteTableName(
      schemaName = Some("schema"),
      tableName = "tableName",
      tableAlias = Some("t")
    )
    assertResult(Parsed.Success(expected, 21)) { parse("schema.tableName AS t", TableOrSub.tableName(_)) }
  }
  "schema.tableName  t" should "be parsed as valid table name with alias but without AS" in {
    val expected = SqliteTableName(
      schemaName = Some("schema"),
      tableName = "tableName",
      tableAlias = Some("t")
    )
    assertResult(Parsed.Success(expected, 19)) { parse("schema.tableName  t", TableOrSub.tableName(_)) }
  }
  "schema.tableName" should "be parsed as valid table name without alias" in {
    val expected = SqliteTableName(schemaName = Some("schema"), tableName = "tableName")
    assertResult(Parsed.Success(expected, 16)) { parse("schema.tableName", TableOrSub.tableName(_)) }
  }
  "tableName" should "be parsed as valid table name without schema and alias" in {
    val expected = SqliteTableName(tableName = "tableName")
    assertResult(Parsed.Success(expected, 9)) { parse("tableName", TableOrSub.tableName(_)) }
  }
  "tableName as t1" should "be parsed as valid table name without schema but with alias" in {
    val expected = SqliteTableName(tableName = "tableName", tableAlias = Some("t1"))
    assertResult(Parsed.Success(expected, 15)) { parse("tableName as t1", TableOrSub.tableName(_)) }
  }
  "tableName t1" should "be parsed as valid table name without schema but with alias without as" in {
    val expected = SqliteTableName(tableName = "tableName", tableAlias = Some("t1"))
    assertResult(Parsed.Success(expected, 12)) { parse("tableName t1", TableOrSub.tableName(_)) }
  }
  "schema.* t1" should "not be parsed as valid table name" in {
    parse("schema.* t1", TableOrSub.tableName(_)) shouldBe a [Parsed.Failure]
  }
  "schema.table AS  " should "not be parsed as valid table name - missing alias name" in {
    parse("schema.table AS  ", TableOrSub.tableName(_)) shouldBe a [Parsed.Failure]
  }
  "schema.as as as" should "not be parsed as valid table name - missing alias name" in {
    parse("schema.table AS  ", TableOrSub.tableName(_)) shouldBe a [Parsed.Failure]
  }
}

class SelectJoinsTests extends UnitSpec {
  "ON 1 = 1" should "be parsed as valid join constraint" in {
    val expected = SqliteJoinConstraint(
      joinExpression = Some(SqliteBinaryOp(EQUAL, SqliteIntegerLit(1), SqliteIntegerLit(1)))
    )
    assertResult(Parsed.Success(expected, 8)) { parse("ON 1 = 1", Joins.joinConstrain(_)) }
  }
  "ON t1.id > t2.id" should "be parsed as valid join constraint" in {
    val expected = SqliteJoinConstraint(
      joinExpression = Some(
        SqliteBinaryOp(
          op = GREATER_THEN,
          left = SqliteColumnExpr(tableName = Some("t1"), columnName = "id"),
          right = SqliteColumnExpr(tableName = Some("t2"), columnName = "id"),
        )
      )
    )
    assertResult(Parsed.Success(expected, 16)) { parse("ON t1.id > t2.id", Joins.joinConstrain(_)) }
  }
  "Using ( id )" should "be parsed as valid join constraint" in {
    val expected = SqliteJoinConstraint(byColumnNames = List("id"))
    assertResult(Parsed.Success(expected, 12)) { parse("Using ( id )", Joins.joinConstrain(_)) }
  }
  "Using ( id,   keyCol )" should "be parsed as valid join constraint" in {
    val expected = SqliteJoinConstraint(byColumnNames = List("id", "keyCol"))
    assertResult(Parsed.Success(expected, 22)) { parse("Using ( id,   keyCol )", Joins.joinConstrain(_)) }
  }
  "Using ( id, crap" should "be parsed as valid join constraint" in {
    val expected = SqliteJoinConstraint(byColumnNames = List("id"))
    assertResult(Parsed.Success(expected, 12)) { parse("Using ( id )", Joins.joinConstrain(_)) }
  }
  "Using (id,   crap" should "not be parsed as valid join constraint - missing close parenthesis" in {
    parse("Using (id,   crap", Joins.joinConstrain(_)) shouldBe a [Parsed.Failure]
  }
}
