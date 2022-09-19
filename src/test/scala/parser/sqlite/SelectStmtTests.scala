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
    val expected = SqliteTableOrSubquery(
      table = Some(
        SqliteTableName(
          schemaName = Some("schema"),
          tableName = "tableName",
          tableAlias = Some("t")
        )
      )
    )
    assertResult(Parsed.Success(expected, 21)) { parse("schema.tableName AS t", TableOrSub.tableName(_)) }
  }
  "schema.tableName  t" should "be parsed as valid table name with alias but without AS" in {
    val expected = SqliteTableOrSubquery(
      table = Some(
        SqliteTableName(
          schemaName = Some("schema"),
          tableName = "tableName",
          tableAlias = Some("t")
        )
      )
    )
    assertResult(Parsed.Success(expected, 19)) { parse("schema.tableName  t", TableOrSub.tableName(_)) }
  }
  "schema.tableName" should "be parsed as valid table name without alias" in {
    val expected = SqliteTableOrSubquery(table = Some(SqliteTableName(schemaName = Some("schema"), tableName = "tableName")))
    assertResult(Parsed.Success(expected, 16)) { parse("schema.tableName", TableOrSub.tableName(_)) }
  }
  "tableName" should "be parsed as valid table name without schema and alias" in {
    val expected = SqliteTableOrSubquery(table = Some(SqliteTableName(tableName = "tableName")))
    assertResult(Parsed.Success(expected, 9)) { parse("tableName", TableOrSub.tableName(_)) }
  }
  "tableName as t1" should "be parsed as valid table name without schema but with alias" in {
    val expected = SqliteTableOrSubquery(table = Some(SqliteTableName(tableName = "tableName", tableAlias = Some("t1"))))
    assertResult(Parsed.Success(expected, 15)) { parse("tableName as t1", TableOrSub.tableName(_)) }
  }
  "tableName t1" should "be parsed as valid table name without schema but with alias without as" in {
    val expected = SqliteTableOrSubquery(table = Some(SqliteTableName(tableName = "tableName", tableAlias = Some("t1"))))
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
  "Using (id,   crap" should "not be parsed as valid join constraint - missing close parenthesis" in {
    parse("Using (id,   crap", Joins.joinConstrain(_)) shouldBe a [Parsed.Failure]
  }

  "Inner Join" should "be parsed as INNER JOIN" in {
    assertResult(Parsed.Success(SqliteJoinInner(false), 10)) { parse("Inner Join", Joins.joinOperator(_)) }
  }
  "inner   JOIN" should "be parsed as INNER JOIN" in {
    assertResult(Parsed.Success(SqliteJoinInner(false), 12)) { parse("inner   JOIN", Joins.joinOperator(_)) }
  }
  "NATURAL inner   JOIN" should "be parsed as INNER JOIN" in {
    assertResult(Parsed.Success(SqliteJoinInner(false), 20)) { parse("NATURAL inner   JOIN", Joins.joinOperator(_)) }
  }
  "," should "be parser as inner join operator" in {
    assertResult(Parsed.Success(SqliteJoinInner(true), 1)) { parse(",", Joins.joinOperator(_)) }
  }
  "LEFT  JOIN" should "be parsed as LEFT JOIN" in {
    assertResult(Parsed.Success(SqliteJoinLeft(), 10)) { parse("LEFT  JOIN", Joins.joinOperator(_)) }
  }
  "LEFT  Outer join" should "be parsed as LEFT JOIN" in {
    assertResult(Parsed.Success(SqliteJoinLeft(), 16)) { parse("LEFT  Outer join", Joins.joinOperator(_)) }
  }
  "nAtural LEFT  Outer join" should "be parsed as LEFT JOIN" in {
    assertResult(Parsed.Success(SqliteJoinLeft(), 24)) { parse("nAtural LEFT  Outer join", Joins.joinOperator(_)) }
  }
  "right  JOIN" should "be parsed as RIGHT JOIN" in {
    assertResult(Parsed.Success(SqliteJoinRight(), 11)) { parse("right  JOIN", Joins.joinOperator(_)) }
  }
  "right  OUTER join" should "be parsed as RIGHT JOIN" in {
    assertResult(Parsed.Success(SqliteJoinRight(), 17)) { parse("right  OUTER join", Joins.joinOperator(_)) }
  }
  "nAtural Right  outer join" should "be parsed as RIGHT JOIN" in {
    assertResult(Parsed.Success(SqliteJoinRight(), 25)) { parse("nAtural Right  outer join", Joins.joinOperator(_)) }
  }
  "full  join" should "be parsed as FULL JOIN" in {
    assertResult(Parsed.Success(SqliteJoinFull(), 10)) { parse("full  join", Joins.joinOperator(_)) }
  }
  "Full  Outer join" should "be parsed as FULL JOIN" in {
    assertResult(Parsed.Success(SqliteJoinFull(), 16)) { parse("Full  Outer join", Joins.joinOperator(_)) }
  }
  "nAtural fuLL  Outer join" should "be parsed as FULL JOIN" in {
    assertResult(Parsed.Success(SqliteJoinFull(), 24)) { parse("nAtural fuLL  Outer join", Joins.joinOperator(_)) }
  }
  "cross  join" should "be parsed as CROSS JOIN" in {
    assertResult(Parsed.Success(SqliteJoinCross(), 11)) { parse("cross  join", Joins.joinOperator(_)) }
  }
  "Inner OUTER join" should "not be parsed as valid INNER JOIN" in {
    parse("Inner OUTER join", Joins.joinOperator(_)) shouldBe a [Parsed.Failure]
  }
  "Natural cROSS join" should "not be parsed as valid CROSS JOIN" in {
    parse("Natural cROSS join", Joins.joinOperator(_)) shouldBe a [Parsed.Failure]
  }

  "tableA a INNER JOIN tableB b ON a.X = b.Z" should "be parsed as INNER JOIN of two tables" in {
    val constrainExpr = SqliteBinaryOp(
      op = EQUAL,
      left = SqliteColumnExpr(tableName = Some("a"), columnName = "X"),
      right = SqliteColumnExpr(tableName = Some("b"), columnName = "Z")
    )
    val expected = SqliteJoinExpr(
      firstTable = SqliteTableOrSubquery(table = Some(SqliteTableName(tableName = "tableA", tableAlias = Some("a")))),
      otherJoins = List(
        (
          SqliteJoinInner(false),
          SqliteTableOrSubquery(table = Some(SqliteTableName(tableName = "tableB", tableAlias = Some("b")))),
          SqliteJoinConstraint(Some(constrainExpr))
        )
      )
    )
    assertResult(Parsed.Success(expected, 41)) { parse("tableA a INNER JOIN tableB b ON a.X = b.Z", Joins.joinExpr(_)) }
  }
  "tableA a LEFT  join tableB b ON a.X = b.Z anD a.Y > b.A" should "be parsed as LEFT JOIN of two tables" in {
    val constrainExpr = SqliteBinaryOp(
      op = AND,
      left = SqliteBinaryOp(
        op = EQUAL,
        left = SqliteColumnExpr(tableName = Some("a"), columnName = "X"),
        right = SqliteColumnExpr(tableName = Some("b"), columnName = "Z")
      ),
      right = SqliteBinaryOp(
        op = GREATER_THEN,
        left = SqliteColumnExpr(tableName = Some("a"), columnName = "Y"),
        right = SqliteColumnExpr(tableName = Some("b"), columnName = "A")
      )
    )
    val expected = SqliteJoinExpr(
      firstTable = SqliteTableOrSubquery(table = Some(SqliteTableName(tableName = "tableA", tableAlias = Some("a")))),
      otherJoins = List(
        (
          SqliteJoinLeft(),
          SqliteTableOrSubquery(table = Some(SqliteTableName(tableName = "tableB", tableAlias = Some("b")))),
          SqliteJoinConstraint(Some(constrainExpr))
        )
      )
    )
    assertResult(Parsed.Success(expected, 55)) { parse("tableA a LEFT  join tableB b ON a.X = b.Z anD a.Y > b.A", Joins.joinExpr(_)) }
  }
}

class SelectWhereTests extends UnitSpec {
  "Where x > 1" should "be parsed as valid WHERE condition" in {
    val expected = SqliteWhereExpr(
      condition = SqliteBinaryOp(
        op = GREATER_THEN,
        left = SqliteColumnExpr(columnName = "x"),
        right = SqliteIntegerLit(1)
      )
    )
    assertResult(Parsed.Success(expected, 11)) { parse("Where x > 1", whereExpr(_)) }
  }
  "WHERE   1 = 1" should "be parsed as valid WHERE condition" in {
    val expected = SqliteWhereExpr(
      condition = SqliteBinaryOp(
        op = EQUAL,
        left = SqliteIntegerLit(1),
        right = SqliteIntegerLit(1)
      )
    )
    assertResult(Parsed.Success(expected, 13)) { parse("WHERE   1 = 1", whereExpr(_)) }
  }
}

class SelectGroupByTests extends UnitSpec {
  "Group By col1" should "be parsed as valid GROUP BY clause" in {
    val expected = SqliteGroupByExpr(
      groupingExprs = Seq(SqliteColumnExpr(columnName = "col1"))
    )
    assertResult(Parsed.Success(expected, 13)) { parse("Group By col1", groupByExpr(_)) }
  }
  "GROUP BY col1 + col2" should "be parsed as valid GROUP BY clause" in {
    val expected = SqliteGroupByExpr(
      groupingExprs = Seq(
        SqliteBinaryOp(
          op = ADD,
          left = SqliteColumnExpr(columnName = "col1"),
          right = SqliteColumnExpr(columnName = "col2")
        )
      )
    )
    assertResult(Parsed.Success(expected, 20)) { parse("GROUP BY col1 + col2", groupByExpr(_)) }
  }
  "group by  col1,   col2" should "be parsed as valid GROUP BY clause" in {
    val expected = SqliteGroupByExpr(
      groupingExprs = Seq(
        SqliteColumnExpr(columnName = "col1"),
        SqliteColumnExpr(columnName = "col2")
      )
    )
    assertResult(Parsed.Success(expected, 22)) { parse("group by  col1,   col2", groupByExpr(_)) }
  }
  """group by
      col1,
      col2,
        col1 +  col2""" should "be parsed as valid GROUP BY clause" in {
    val input = """group by
      col1,
      col2,
        col1 +  col2"""
    val expected = SqliteGroupByExpr(
      groupingExprs = Seq(
        SqliteColumnExpr(columnName = "col1"),
        SqliteColumnExpr(columnName = "col2"),
        SqliteBinaryOp(
          op = ADD,
          left = SqliteColumnExpr(columnName = "col1"),
          right = SqliteColumnExpr(columnName = "col2")
        )
      )
    )
    assertResult(Parsed.Success(expected, input.length)) { parse(input, groupByExpr(_)) }
  }
}

class SelectHavingTests extends UnitSpec {
  "HAVING COUNT(col1) > 1" should "be parsed as valid HAVING condition" in {
    val expected = SqliteHavingExpr(
      condition = SqliteBinaryOp(
        op = GREATER_THEN,
        left = SqliteFuncCall(
          func = "COUNT",
          args = List(SqliteColumnExpr(columnName = "col1"))
        ),
        right = SqliteIntegerLit(1)
      )
    )
    assertResult(Parsed.Success(expected, 22)) { parse("HAVING COUNT(col1) > 1", havingExpr(_)) }
  }
  "having min(col1) = max(col1)" should "be parsed as valid HAVING condition" in {
    val expected = SqliteHavingExpr(
      condition = SqliteBinaryOp(
        op = EQUAL,
        left = SqliteFuncCall(
          func = "min",
          args = List(SqliteColumnExpr(columnName = "col1"))
        ),
        right = SqliteFuncCall(
          func = "max",
          args = List(SqliteColumnExpr(columnName = "col1"))
        )
      )
    )
    assertResult(Parsed.Success(expected, 28)) { parse("having min(col1) = max(col1)", havingExpr(_)) }
  }
}

class SelectSetOpTests extends UnitSpec {
  "SELECT 1 AS A UNION ALL SELECT 1 AS A" should "be parsed as simple SELECT statement" in {
    val sel = SqliteSelectCore(
      selectCols = SqliteSelectColumns(
        cols = Seq(
          SqliteResultCol(colExpr = Some(SqliteIntegerLit(1)), colAlias = Some("A"))
        )
      ),
    )
    val expected = SqliteSelectCore(
      selectCols = sel.selectCols,
      setOp = Some(SqliteSetExpr(SqliteUnionAll(), sel))
    )
    assertResult(Parsed.Success(expected, 37)) { parse("SELECT 1 AS A UNION ALL SELECT 1 AS A", SelectCore.selectCore(_)) }
  }
  """
    select col1 from table1
    except
    select col1 from table2
  """ should "be parsed as simple SELECT statement with EXCEPT" in {
    val input = """select col1 from table1
      except
      select col1 from table2
    """
    def sel(tableName: String) = SqliteSelectCore(
      selectCols = SqliteSelectColumns(
        cols = Seq(SqliteResultCol(colExpr = Some(SqliteColumnExpr(columnName = "col1"))))
      ),
      from = Some(SqliteSelectFrom(tableOrSubquery = Some(SqliteTableOrSubquery(table = Some(SqliteTableName(tableName = tableName))))))
    )
    val expected = SqliteSelectCore(
      selectCols = sel("table1").selectCols,
      from = sel("table1").from,
      setOp = Some(SqliteSetExpr(SqliteExcept(), sel("table2")))
    )
    assertResult(Parsed.Success(expected, input.length)) { parse(input, SelectCore.selectCore(_)) }
  }
  """
    select col1 from table1
    except
    select col1 from table2
    union all
    select col1 from table3
  """ should "be parsed as simple SELECT statement with EXCEPT and UNION ALL" in {
    val input = """select col1 from table1
      except
      select col1 from table2
      union all
      select col1 from table3
    """
    val sel3 = SqliteSelectCore(
      selectCols = SqliteSelectColumns(
        cols = Seq(SqliteResultCol(colExpr = Some(SqliteColumnExpr(columnName = "col1"))))
      ),
      from = Some(SqliteSelectFrom(tableOrSubquery = Some(SqliteTableOrSubquery(table = Some(SqliteTableName(tableName = "table3"))))))
    )
    val sel2 = SqliteSelectCore(
      selectCols = SqliteSelectColumns(
        cols = Seq(SqliteResultCol(colExpr = Some(SqliteColumnExpr(columnName = "col1"))))
      ),
      from = Some(SqliteSelectFrom(tableOrSubquery = Some(SqliteTableOrSubquery(table = Some(SqliteTableName(tableName = "table2")))))),
      setOp = Some(SqliteSetExpr(SqliteUnionAll(), sel3))
    )
    val expected = SqliteSelectCore(
      selectCols = SqliteSelectColumns(
        cols = Seq(SqliteResultCol(colExpr = Some(SqliteColumnExpr(columnName = "col1"))))
      ),
      from = Some(SqliteSelectFrom(tableOrSubquery = Some(SqliteTableOrSubquery(table = Some(SqliteTableName(tableName = "table1")))))),
      setOp = Some(SqliteSetExpr(SqliteExcept(), sel2))
    )
    assertResult(Parsed.Success(expected, input.length)) { parse(input, SelectCore.selectCore(_)) }
  }
}

class SelectCoreTests extends UnitSpec {
  "SELECT 1 AS A" should "be parsed as simple SELECT statement" in {
    val expected = SqliteSelectCore(
      selectCols = SqliteSelectColumns(
        cols = Seq(
          SqliteResultCol(colExpr = Some(SqliteIntegerLit(1)), colAlias = Some("A"))
        )
      ),
    )
    assertResult(Parsed.Success(expected, 13)) { parse("SELECT 1 AS A", SelectCore.selectCore(_)) }
  }
  "SELECT a, b FROM tableA" should "be parsed as simple SELECT statement" in {
    val expected = SqliteSelectCore(
      selectCols = SqliteSelectColumns(
        cols = Seq(
          SqliteResultCol(colExpr = Some(SqliteColumnExpr(columnName = "a"))),
          SqliteResultCol(colExpr = Some(SqliteColumnExpr(columnName = "b"))),
        )
      ),
      from = Some(SqliteSelectFrom(
        tableOrSubquery = Some(
          SqliteTableOrSubquery(
            table = Some(SqliteTableName(tableName = "tableA")))
          )
      ))
    )
    assertResult(Parsed.Success(expected, 23)) { parse("SELECT a, b FROM tableA", SelectCore.selectCore(_)) }
  }
  "SELECT distinct  a, b FROM tableA" should "be parsed as simple SELECT statement with distinct" in {
    val expected = SqliteSelectCore(
      selectCols = SqliteSelectColumns(
        distinct = true,
        cols = Seq(
          SqliteResultCol(colExpr = Some(SqliteColumnExpr(columnName = "a"))),
          SqliteResultCol(colExpr = Some(SqliteColumnExpr(columnName = "b"))),
        )
      ),
      from = Some(SqliteSelectFrom(
        tableOrSubquery = Some(
          SqliteTableOrSubquery(
            table = Some(SqliteTableName(tableName = "tableA"))
          )
        )
      ))
    )
    assertResult(Parsed.Success(expected, 33)) { parse("SELECT distinct  a, b FROM tableA", SelectCore.selectCore(_)) }
  }
  "Select a, b  from tableA where a > b" should "be parsed as simple SELECT statement with WHERE" in {
    val expected = SqliteSelectCore(
      selectCols = SqliteSelectColumns(
        cols = Seq(
          SqliteResultCol(colExpr = Some(SqliteColumnExpr(columnName = "a"))),
          SqliteResultCol(colExpr = Some(SqliteColumnExpr(columnName = "b"))),
        )
      ),
      from = Some(SqliteSelectFrom(
        tableOrSubquery = Some(
          SqliteTableOrSubquery(
            table = Some(SqliteTableName(tableName = "tableA"))
          )
        )
      )),
      where = Some(
        SqliteWhereExpr(
          condition = SqliteBinaryOp(
            op = GREATER_THEN,
            left = SqliteColumnExpr(columnName = "a"),
            right = SqliteColumnExpr(columnName = "b")
          )
        )
      )
    )
    assertResult(Parsed.Success(expected, 36)) { parse("Select a, b  from tableA where a > b", SelectCore.selectCore(_)) }
  }
  """
    SELECT
      a.a,
      b.b,
      SUM(a.a + b.b * 10) AS C
    FROM
      tableA a
    INNER JOIN
      tableB b ON a.ID = b.ID
    WHERE
      a.a > a.b
    GROUP BY
      a.a,
      b.b
    HAVING
      COUNT(a.c) > 1
  """ should "be parsed as full SELECT core statement" in {
    val input = """SELECT
      a.a,
      b.b,
      SUM(a.a + b.b * 10) AS C
    FROM
      tableA a
    INNER JOIN
      tableB b ON a.ID = b.ID
    WHERE
      a.a > b.b
    GROUP BY
      a.a,
      b.b
    HAVING
      COUNT(a.c) > 1 """
    val a = SqliteColumnExpr(tableName = Some("a"), columnName = "a")
    val b = SqliteColumnExpr(tableName = Some("b"), columnName = "b")
    val aResCol = SqliteResultCol(colExpr = Some(a))
    val bResCol = SqliteResultCol(colExpr = Some(b))
    val sumFunc = SqliteFuncCall(
      func = "SUM",
      args = List(
        SqliteBinaryOp(
          op = ADD,
          left = a,
          right = SqliteBinaryOp(
            op = MUL,
            left = b,
            right = SqliteIntegerLit(10)
          )
        )
      )
    )
    val cols = SqliteSelectColumns(
        cols = Seq(aResCol, bResCol,
          SqliteResultCol(
            colExpr = Some(sumFunc),
            colAlias = Some("C")
          )
        )
      )
    val joins = SqliteJoinExpr(
      firstTable = SqliteTableOrSubquery(
        table = Some(
          SqliteTableName(
            tableName = "tableA",
            tableAlias = Some("a")
          )
        )
      ),
      otherJoins = Seq(
        (
          SqliteJoinInner(),
          SqliteTableOrSubquery(
            table = Some(
              SqliteTableName(
                tableName = "tableB",
                tableAlias = Some("b")
              )
            )
          ),
          SqliteJoinConstraint(
            joinExpression = Some(
              SqliteBinaryOp(
                op = EQUAL,
                left = SqliteColumnExpr(tableName = Some("a"), columnName = "ID"),
                right = SqliteColumnExpr(tableName = Some("b"), columnName = "ID"),
              )
            )
          )
        )
      )
    )
    val having = SqliteHavingExpr(
      condition = SqliteBinaryOp(
        op = GREATER_THEN,
        left = SqliteFuncCall(
          func = "COUNT",
          args = List(SqliteColumnExpr(tableName = Some("a"), columnName = "c"))
        ),
        right = SqliteIntegerLit(1)
      )
    )

    val expected = SqliteSelectCore(
      selectCols = cols,
      from = Some(SqliteSelectFrom(joinExpr = Some(joins))),
      where = Some(SqliteWhereExpr(condition = SqliteBinaryOp(op = GREATER_THEN, left = a, right = b))),
      groupBy = Some(SqliteGroupByExpr(Seq(a, b))),
      having = Some(having)
    )
    assertResult(Parsed.Success(expected, input.length)) { parse(input, SelectCore.selectCore(_)) }
  }
}

class SelectSubqueryTests extends UnitSpec {
  """
  SELECT
    x.col1 + x.col2 AS newX
  FROM
  (
    SELECT
      1 AS col1,
      2 AS col2
  ) x
  """ should "be parsed as simple SELECT statement" in {
    val input = """SELECT
        x.col1 + x.col2 AS newX
      FROM
      (
        SELECT
          1 AS col1,
          2 AS col2
      ) x
    """
    val cols = SqliteSelectColumns(
      cols = Seq(
        SqliteResultCol(
          colExpr = Some(
            SqliteBinaryOp(
              op = ADD,
              left = SqliteColumnExpr(tableName = Some("x"), columnName = "col1"),
              right = SqliteColumnExpr(tableName = Some("x"), columnName = "col2"),
            )
          ),
          colAlias = Some("newX")
        )
      )
    )
    val subQuerySelect = SqliteSelectCore(
      selectCols = SqliteSelectColumns(
        cols = Seq(
          SqliteResultCol(colExpr = Some(SqliteIntegerLit(1)), colAlias = Some("col1")),
          SqliteResultCol(colExpr = Some(SqliteIntegerLit(2)), colAlias = Some("col2")),
        )
      )
    )
    val from = SqliteSelectFrom(
      tableOrSubquery = Some(
        SqliteTableOrSubquery(
          subQuery = Some(
            SqliteSelectSubquery(
              subQuery = subQuerySelect,
              alias = Some("x")
            )
          )
        )
      )
    )
    val expected = SqliteSelectCore(
      selectCols = cols,
      from = Some(from),
    )
    assertResult(Parsed.Success(expected, input.length)) { parse(input, SelectCore.selectCore(_)) }
  }
}

class SelectSingleCteTests extends UnitSpec {
  "tmp1 AS (SELECT 1 AS A)" should "be parsed as simple single CTE" in {
    val input = "tmp1 AS (SELECT 1 AS A)"
    val sel = SqliteSelectCore(
      selectCols = SqliteSelectColumns(
        cols = Seq(
          SqliteResultCol(colExpr = Some(SqliteIntegerLit(1)), colAlias = Some("A"))
        )
      )
    )
    val expected = SqliteCommonTableExpr(cteName = "tmp1", cteBody = sel)
    assertResult(Parsed.Success(expected, input.length)) { parse(input, CTE.singleCTE(_)) }
  }
  "tmp1 (A)  AS (SELECT 1 AS A)" should "be parsed as simple single CTE" in {
    val input = "tmp1 (A)  AS (SELECT 1 AS A)"
    val sel = SqliteSelectCore(
      selectCols = SqliteSelectColumns(
        cols = Seq(
          SqliteResultCol(colExpr = Some(SqliteIntegerLit(1)), colAlias = Some("A"))
        )
      )
    )
    val expected = SqliteCommonTableExpr(cteName = "tmp1", cteColNames = Some(Seq("A")), cteBody = sel)
    assertResult(Parsed.Success(expected, input.length)) { parse(input, CTE.singleCTE(_)) }
  }
  "tmp1 (A, colB)  AS (SELECT 1 AS A, 2 colB)" should "be parsed as simple single CTE" in {
    val input = "tmp1 (A, colB)  AS (SELECT 1 AS A, 2 colB)"
    val sel = SqliteSelectCore(
      selectCols = SqliteSelectColumns(
        cols = Seq(
          SqliteResultCol(colExpr = Some(SqliteIntegerLit(1)), colAlias = Some("A")),
          SqliteResultCol(colExpr = Some(SqliteIntegerLit(2)), colAlias = Some("colB"))
        )
      )
    )
    val expected = SqliteCommonTableExpr(cteName = "tmp1", cteColNames = Some(Seq("A", "colB")), cteBody = sel)
    assertResult(Parsed.Success(expected, input.length)) { parse(input, CTE.singleCTE(_)) }
  }
  "tmp1 AS  materialized (SELECT 1 AS A)" should "be parsed as simple single CTE" in {
    val input = "tmp1 AS  materialized (SELECT 1 AS A)"
    val sel = SqliteSelectCore(
      selectCols = SqliteSelectColumns(
        cols = Seq(
          SqliteResultCol(colExpr = Some(SqliteIntegerLit(1)), colAlias = Some("A"))
        )
      )
    )
    val expected = SqliteCommonTableExpr(cteName = "tmp1", isMaterialized = true, cteBody = sel)
    assertResult(Parsed.Success(expected, input.length)) { parse(input, CTE.singleCTE(_)) }
  }
}
