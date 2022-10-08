import org.scalatest.flatspec._
import org.scalatest.matchers._
import fastparse._, NoWhitespace._
import dev.dskrzypiec.parser.sqlite._
import dev.dskrzypiec.parser.sqlite.Expr.Literal._
import dev.dskrzypiec.parser.sqlite.Print

class PrintBinaryOpTests extends UnitSpec {
  "1 + 2" should "be printed" in {
    val input = SqliteBinaryOp(ADD, SqliteIntegerLit(1), SqliteIntegerLit(2))
    val expected = "1 + 2"
    assertResult(expected) { Print.binaryOp(input) }
  }
  "'x' || 'y'" should "be printed" in {
    val input = SqliteBinaryOp(CONCAT, SqliteStringLit("x"), SqliteStringLit("y"))
    val expected = "'x' || 'y'"
    assertResult(expected) { Print.binaryOp(input) }
  }
  "1 > 0 AND 2 > 1" should "be printed" in {
    val input = SqliteBinaryOp(
      op = AND,
      left = SqliteBinaryOp(GREATER_THEN, SqliteIntegerLit(1), SqliteIntegerLit(0)),
      right = SqliteBinaryOp(GREATER_THEN, SqliteIntegerLit(2), SqliteIntegerLit(1))
    )
    val expected = "1 > 0 AND 2 > 1"
    assertResult(expected) { Print.binaryOp(input) }
  }
}

class PrintFuncCallTests extends UnitSpec {
  "SUM(col1)" should "be printed" in {
    val input = SqliteFuncCall(func = "SUM", args = List(SqliteColumnExpr(columnName = "col1")))
    val expected = "SUM(col1)"
    assertResult(expected) { Print.funcCall(input) }
  }
  "COUNT(*)" should "be printed" in {
    val input = SqliteFuncCall(func = "COUNT", starAsArg = true)
    val expected = "COUNT(*)"
    assertResult(expected) { Print.funcCall(input) }
  }
  "COUNT(col1, col2)" should "be printed" in {
    val input = SqliteFuncCall(
      func = "COUNT",
      args = List(
        SqliteColumnExpr(columnName = "col1"),
        SqliteColumnExpr(columnName = "col2"),
      )
    )
    val expected = "COUNT(col1, col2)"
    assertResult(expected) { Print.funcCall(input) }
  }
  "COUNT(DISTINCT x1)" should "be printed" in {
    val input = SqliteFuncCall(
      func = "COUNT",
      args = List(SqliteColumnExpr(columnName = "x1")),
      distinctArgs = true
    )
    val expected = "COUNT(DISTINCT x1)"
    assertResult(expected) { Print.funcCall(input) }
  }
}

class PrintCaseExprTests extends UnitSpec {
  "CASE WHEN 1 > 0 THEN 10 ELSE -1 END" should "be printed" in {
    val input = SqliteCaseExpr(
      whenThens = List(
        SqliteCaseWhenThen(
          when = SqliteBinaryOp(
            op = GREATER_THEN,
            left = SqliteIntegerLit(1),
            right = SqliteIntegerLit(0)
          ),
          then = SqliteIntegerLit(10)
        )
      ),
      elseExpr = Some(SqliteIntegerLit(-1))
    )
    val expected =
"""CASE
    WHEN 1 > 0 THEN 10
    ELSE -1
END"""
    assertResult(expected) { Print.caseExpr(input) }
  }
  "CASE WHEN 1 > 0 THEN 10 ELSE -1 END" should "be printed with indent = 1" in {
    val input = SqliteCaseExpr(
      whenThens = List(
        SqliteCaseWhenThen(
          when = SqliteBinaryOp(
            op = GREATER_THEN,
            left = SqliteIntegerLit(1),
            right = SqliteIntegerLit(0)
          ),
          then = SqliteIntegerLit(10)
        )
      ),
      elseExpr = Some(SqliteIntegerLit(-1))
    )
    val expected =
"""    CASE
        WHEN 1 > 0 THEN 10
        ELSE -1
    END"""
    assertResult(expected) { Print.caseExpr(input, 1) }
  }
  "CASE WHEN 1 > 0 THEN 10 ELSE -1 END" should "be printed with indent = 2" in {
    val input = SqliteCaseExpr(
      whenThens = List(
        SqliteCaseWhenThen(
          when = SqliteBinaryOp(
            op = GREATER_THEN,
            left = SqliteIntegerLit(1),
            right = SqliteIntegerLit(0)
          ),
          then = SqliteIntegerLit(10)
        )
      ),
      elseExpr = Some(SqliteIntegerLit(-1))
    )
    val expected =
"""        CASE
            WHEN 1 > 0 THEN 10
            ELSE -1
        END"""
    assertResult(expected) { Print.caseExpr(input, 2) }
  }
  "CASE WHEN 0 > 1 THEN 42 WHEN 1 > 0 THEN 10 ELSE -1 END" should "be printed" in {
    val input = SqliteCaseExpr(
      whenThens = List(
        SqliteCaseWhenThen(
          when = SqliteBinaryOp(
            op = GREATER_THEN,
            left = SqliteIntegerLit(0),
            right = SqliteIntegerLit(1)
          ),
          then = SqliteIntegerLit(42)
        ),
        SqliteCaseWhenThen(
          when = SqliteBinaryOp(
            op = GREATER_THEN,
            left = SqliteIntegerLit(1),
            right = SqliteIntegerLit(0)
          ),
          then = SqliteIntegerLit(10)
        )
      ),
      elseExpr = Some(SqliteIntegerLit(-1))
    )
    val expected =
"""CASE
    WHEN 0 > 1 THEN 42
    WHEN 1 > 0 THEN 10
    ELSE -1
END"""
    assertResult(expected) { Print.caseExpr(input) }
  }
}

class PrintColumnResTests extends UnitSpec {
  "1 AS col1" should "be printed" in {
    val input = SqliteResultCol(
      colExpr = Some(SqliteIntegerLit(1)),
      colAlias = Some("col1")
    )
    val expected = "1 AS col1"
    assertResult(expected) { Print.selectResultCol(input) }
  }
  "1 AS col1" should "be printed with indent=1" in {
    val input = SqliteResultCol(
      colExpr = Some(SqliteIntegerLit(1)),
      colAlias = Some("col1")
    )
    val expected = "    1 AS col1"
    assertResult(expected) { Print.selectResultCol(input, 1) }
  }
  "*" should "be printed with indent = 2" in {
    val input = SqliteResultCol(isStar = true)
    val expected = "        *"
    assertResult(expected) { Print.selectResultCol(input, 2) }
  }
  "table1.*" should "be printed" in {
    val input = SqliteResultCol(isStarInTableName = Some("table1"))
    val expected = "table1.*"
    assertResult(expected) { Print.selectResultCol(input) }
  }
  "table1.col1" should "be printed with indent=1" in {
    val input = SqliteResultCol(
      colExpr = Some(SqliteColumnExpr(tableName = Some("table1"), columnName = "col1"))
    )
    val expected = "    table1.col1"
    assertResult(expected) { Print.selectResultCol(input, 1) }
  }
  """
    DISTINCT
    col1,
    col2,
    col3
  """ should "be printed with indent=1" in {
    val input = SqliteSelectColumns(
      distinct = true,
      cols = Seq(
        SqliteResultCol(colExpr = Some(SqliteColumnExpr(columnName = "col1"))),
        SqliteResultCol(colExpr = Some(SqliteColumnExpr(columnName = "col2"))),
        SqliteResultCol(colExpr = Some(SqliteColumnExpr(columnName = "col3"))),
      )
    )
    val expected =
"""    DISTINCT
    col1,
    col2,
    col3"""
    assertResult(expected) { Print.selectCols(input, 1) }
  }
}

class PrintWhereExprTests extends UnitSpec {
  "WHERE t1.col1 > 0" should "be printed" in {
    val input = SqliteWhereExpr(
      condition = SqliteBinaryOp(
        op = GREATER_THEN,
        left = SqliteColumnExpr(tableName = Some("t1"), columnName = "col1"),
        right = SqliteIntegerLit(0)
      )
    )
    val expected =
"""WHERE
    t1.col1 > 0"""
    assertResult(expected) { Print.whereExpr(input) }
  }
  "WHERE t1.col1 > 0" should "be printed with indent=2" in {
    val input = SqliteWhereExpr(
      condition = SqliteBinaryOp(
        op = GREATER_THEN,
        left = SqliteColumnExpr(tableName = Some("t1"), columnName = "col1"),
        right = SqliteIntegerLit(0)
      )
    )
    val expected =
"""        WHERE
            t1.col1 > 0"""
    assertResult(expected) { Print.whereExpr(input, 2) }
  }
}

class PrintGroupByExprTests extends UnitSpec {
  "GROUP BY col1, col2, col3" should "be printed with indent=1" in {
    val input = SqliteGroupByExpr(
      groupingExprs = Seq(
        SqliteColumnExpr(columnName = "col1"),
        SqliteColumnExpr(columnName = "col2"),
        SqliteColumnExpr(columnName = "col3"),
      )
    )
    val expected =
"""    GROUP BY
        col1,
        col2,
        col3"""
    assertResult(expected) { Print.groupByExpr(input, 1) }
  }
}

class PrintOrderByExprTests extends UnitSpec {
  "col1 COLLATE UTF8 DESC NULLS FIRST" should "be printed for ordering term" in {
    val input = SqliteOrderingTerm(
      expr = SqliteColumnExpr(columnName = "col1"),
      collationName = Some("UTF8"),
      ascending = false,
      nullsLast = false
    )
    val expected = "col1 COLLATE UTF8 DESC NULLS FIRST"
    assertResult(expected) { Print.orderingTerm(input) }
  }
  "col1 COLLATE UTF8 NULLS FIRST" should "be printed for ordering term" in {
    val input = SqliteOrderingTerm(
      expr = SqliteColumnExpr(columnName = "col1"),
      collationName = Some("UTF8"),
      ascending = true,
      nullsLast = false
    )
    val expected = "col1 COLLATE UTF8 NULLS FIRST"
    assertResult(expected) { Print.orderingTerm(input) }
  }
  "col1 COLLATE UTF8" should "be printed for ordering term" in {
    val input = SqliteOrderingTerm(
      expr = SqliteColumnExpr(columnName = "col1"),
      collationName = Some("UTF8"),
      ascending = true,
      nullsLast = true
    )
    val expected = "col1 COLLATE UTF8"
    assertResult(expected) { Print.orderingTerm(input) }
  }
  "col1" should "be printed for ordering term" in {
    val input = SqliteOrderingTerm(expr = SqliteColumnExpr(columnName = "col1"))
    val expected = "col1"
    assertResult(expected) { Print.orderingTerm(input) }
  }
  """
  ORDER BY
    col1,
    col2 DESC,
    col3 COLLATE UTF8,
    col4 COLLATE UTF8 DESC,
    col5 COLLATE UTF8 DESC NULLS FIRST
  """ should "be printed" in {
    val input = SqliteOrderByExpr(
      Seq(
        SqliteOrderingTerm(expr = SqliteColumnExpr(columnName = "col1")),
        SqliteOrderingTerm(expr = SqliteColumnExpr(columnName = "col2"), ascending = false),
        SqliteOrderingTerm(expr = SqliteColumnExpr(columnName = "col3"), collationName = Some("UTF8")),
        SqliteOrderingTerm(expr = SqliteColumnExpr(columnName = "col4"), collationName = Some("UTF8"), ascending = false),
        SqliteOrderingTerm(expr = SqliteColumnExpr(columnName = "col5"), collationName = Some("UTF8"), ascending = false, nullsLast = false),
      )
    )
    val expected =
"""ORDER BY
    col1,
    col2 DESC,
    col3 COLLATE UTF8,
    col4 COLLATE UTF8 DESC,
    col5 COLLATE UTF8 DESC NULLS FIRST"""
    assertResult(expected) { Print.orderByExpr(input) }
  }
  """
  ORDER BY
    col1,
    col2 DESC,
    col3 COLLATE UTF8,
    col4 COLLATE UTF8 DESC,
    col5 COLLATE UTF8 DESC NULLS FIRST
  """ should "be printed with indent=2" in {
    val input = SqliteOrderByExpr(
      Seq(
        SqliteOrderingTerm(expr = SqliteColumnExpr(columnName = "col1")),
        SqliteOrderingTerm(expr = SqliteColumnExpr(columnName = "col2"), ascending = false),
        SqliteOrderingTerm(expr = SqliteColumnExpr(columnName = "col3"), collationName = Some("UTF8")),
        SqliteOrderingTerm(expr = SqliteColumnExpr(columnName = "col4"), collationName = Some("UTF8"), ascending = false),
        SqliteOrderingTerm(expr = SqliteColumnExpr(columnName = "col5"), collationName = Some("UTF8"), ascending = false, nullsLast = false),
      )
    )
    val expected =
"""        ORDER BY
            col1,
            col2 DESC,
            col3 COLLATE UTF8,
            col4 COLLATE UTF8 DESC,
            col5 COLLATE UTF8 DESC NULLS FIRST"""
    assertResult(expected) { Print.orderByExpr(input, indent = 2) }
  }
}

class PrintLimitExprTests extends UnitSpec {
  "LIMIT 10" should "be printed" in {
    val input = SqliteLimitExpr(SqliteIntegerLit(10))
    val expected =
"""LIMIT
    10"""
    assertResult(expected) { Print.limitExpr(input) }
  }
  "LIMIT 10" should "be printed with indent=2" in {
    val input = SqliteLimitExpr(SqliteIntegerLit(10))
    val expected =
"""        LIMIT
            10"""
    assertResult(expected) { Print.limitExpr(input, 2) }
  }
  "LIMIT 10 + 10" should "be printed" in {
    val input = SqliteLimitExpr(SqliteBinaryOp(ADD, SqliteIntegerLit(10), SqliteIntegerLit(10)))
    val expected =
"""LIMIT
    10 + 10"""
    assertResult(expected) { Print.limitExpr(input) }
  }
  "LIMIT 10 OFFSET 10" should "be printed" in {
    val input = SqliteLimitExpr(SqliteIntegerLit(10), Some(SqliteIntegerLit(10)))
    val expected =
"""LIMIT
    10 OFFSET 10"""
    assertResult(expected) { Print.limitExpr(input) }
  }
}

class PrintJoinExprTests extends UnitSpec {
  "schemaName.tableName AS t1" should "be printed as table name with indent=1" in {
    val input = SqliteTableName(Some("schemaName"), "tableName", Some("t1"))
    val expected = "    schemaName.tableName AS t1"
    assertResult(expected) { Print.tableName(input, 1) }
  }
  "tableName AS t1" should "be printed as table name with indent=2" in {
    val input = SqliteTableName(None, "tableName", Some("t1"))
    val expected = "        tableName AS t1"
    assertResult(expected) { Print.tableName(input, 2) }
  }
  "src.tableName" should "be printed as table name with indent=2" in {
    val input = SqliteTableName(Some("src"), "tableName", None)
    val expected = "        src.tableName"
    assertResult(expected) { Print.tableName(input, 2) }
  }
  "schemaName.tableName AS t1" should "be printed as table name or subquery with indent=1" in {
    val input = SqliteTableOrSubquery(Some(SqliteTableName(Some("schemaName"), "tableName", Some("t1"))))
    val expected = "    schemaName.tableName AS t1"
    assertResult(expected) { Print.tableOrSubquery(input, 1) }
  }
  "src.table1 t1 LEFT JOIN src.table2 t2 ON t1.col1 = t2.col1" should "be printed with indent=1" in {
    val input = SqliteJoinExpr(
      firstTable = SqliteTableOrSubquery(Some(SqliteTableName(Some("src"), "table1", Some("t1")))),
      otherJoins = Seq(
        (
          SqliteJoinLeft(),
          SqliteTableOrSubquery(Some(SqliteTableName(Some("src"), "table2", Some("t2")))),
          SqliteJoinConstraint(
            joinExpression = Some(SqliteBinaryOp(EQUAL, SqliteColumnExpr(None, Some("t1"), "col1"), SqliteColumnExpr(None, Some("t2"), "col1")))
          )
        )
      )
    )
    val expected =
"""        src.table1 AS t1
    LEFT JOIN
        src.table2 AS t2 ON t1.col1 = t2.col1"""
    assertResult(expected) { Print.joinExpr(input, 1) }
  }
  "joins of 3 tables" should "be printed with indent=2" in {
    val input = SqliteJoinExpr(
      firstTable = SqliteTableOrSubquery(Some(SqliteTableName(Some("src"), "table1", Some("t1")))),
      otherJoins = Seq(
        (
          SqliteJoinLeft(),
          SqliteTableOrSubquery(Some(SqliteTableName(Some("src"), "table2", Some("t2")))),
          SqliteJoinConstraint(
            joinExpression = Some(SqliteBinaryOp(EQUAL, SqliteColumnExpr(None, Some("t1"), "col1"), SqliteColumnExpr(None, Some("t2"), "col1")))
          )
        ),
        (
          SqliteJoinInner(false),
          SqliteTableOrSubquery(Some(SqliteTableName(Some("model"), "table3", Some("mt")))),
          SqliteJoinConstraint(
            joinExpression = Some(SqliteBinaryOp(LESS_THEN, SqliteColumnExpr(None, Some("mt"), "col2"), SqliteColumnExpr(None, Some("t2"), "col2")))
          )
        )
      )
    )
    val expected =
"""            src.table1 AS t1
        LEFT JOIN
            src.table2 AS t2 ON t1.col1 = t2.col1
        INNER JOIN
            model.table3 AS mt ON mt.col2 < t2.col2"""
    assertResult(expected) { Print.joinExpr(input, 2) }
  }
}

class PrintSelectFromExprTests extends UnitSpec {
  // TODO
}
