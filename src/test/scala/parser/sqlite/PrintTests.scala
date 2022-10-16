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
  "FROM schemaName.tableName" should "be printed as FROM with indent=1" in {
    val input = SqliteSelectFrom(
      tableOrSubquery = Some(SqliteTableOrSubquery(Some(SqliteTableName(Some("schemaName"), "tableName"))))
    )
    val expected =
"""    FROM
        schemaName.tableName"""
    assertResult(expected) { Print.selectFrom(input, 1) }
  }
  "FROM schemaName.tableName" should "be printed as FROM with indent=2" in {
    val input = SqliteSelectFrom(
      tableOrSubquery = Some(SqliteTableOrSubquery(Some(SqliteTableName(Some("schemaName"), "tableName"))))
    )
    val expected =
"""        FROM
            schemaName.tableName"""
    assertResult(expected) { Print.selectFrom(input, 2) }
  }
  "FROM src.table1 t1 LEFT JOIN src.table2 t2 ON t1.col1 = t2.col1" should "be printed with indent=1" in {
    val inputJoin = SqliteJoinExpr(
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
    val input = SqliteSelectFrom(joinExpr = Some(inputJoin))
    val expected =
"""    FROM
        src.table1 AS t1
    LEFT JOIN
        src.table2 AS t2 ON t1.col1 = t2.col1"""
    assertResult(expected) { Print.selectFrom(input, 1) }
  }
}

class PrintSelectSetOpTests extends UnitSpec {
  "SELECT col1 FROM table1 UNION ALL SELECT col2 FROM table2" should "be printed as SET op with indent=1" in {
    val sel2 = SqliteSelectCore(
      selectCols = SqliteSelectColumns(
        cols = Seq(SqliteResultCol(colExpr = Some(SqliteColumnExpr(columnName = "col2"))))
      ),
      from = Some(SqliteSelectFrom(tableOrSubquery = Some(SqliteTableOrSubquery(table = Some(SqliteTableName(tableName = "table2"))))))
    )
    val input = SqliteSetExpr(SqliteUnionAll(), sel2)
    val expected =
"""    UNION ALL
    SELECT
        col2
    FROM
        table2
"""
    assertResult(expected) { Print.setOpExpr(input, 1) }
  }
}

class PrintSelectCoreTests extends UnitSpec {
  "SELECT col1, col2 FROM src.table1 WHERE col1 > 42" should "be printed as SELECT core with indent=1" in {
    val input = SqliteSelectCore(
      selectCols = SqliteSelectColumns(
        cols = Seq(
          SqliteResultCol(colExpr = Some(SqliteColumnExpr(columnName = "col1"))),
          SqliteResultCol(colExpr = Some(SqliteColumnExpr(columnName = "col2"))),
        )
      ),
      from = Some(SqliteSelectFrom(
        tableOrSubquery = Some(
          SqliteTableOrSubquery(
            table = Some(SqliteTableName(schemaName = Some("src"), tableName = "table1"))
          )
        )
      )),
      where = Some(
        SqliteWhereExpr(
          condition = SqliteBinaryOp(
            op = GREATER_THEN,
            left = SqliteColumnExpr(columnName = "col1"),
            right = SqliteIntegerLit(42)
          )
        )
      )
    )
    val expected =
"""    SELECT
        col1,
        col2
    FROM
        src.table1
    WHERE
        col1 > 42
"""
    assertResult(expected) { Print.selectCore(input, 1) }
  }
  "SELECT col1 FROM table1 UNION ALL SELECT col2 FROM table2" should "be printed as SET op with indent=1" in {
    val sel2 = SqliteSelectCore(
      selectCols = SqliteSelectColumns(
        cols = Seq(SqliteResultCol(colExpr = Some(SqliteColumnExpr(columnName = "col2"))))
      ),
      from = Some(SqliteSelectFrom(tableOrSubquery = Some(SqliteTableOrSubquery(table = Some(SqliteTableName(tableName = "table2"))))))
    )
    val input = SqliteSelectCore(
      selectCols = SqliteSelectColumns(
        cols = Seq(SqliteResultCol(colExpr = Some(SqliteColumnExpr(columnName = "col1"))))
      ),
      from = Some(SqliteSelectFrom(tableOrSubquery = Some(SqliteTableOrSubquery(table = Some(SqliteTableName(tableName = "table1")))))),
      setOp = Some(SqliteSetExpr(SqliteUnionAll(), sel2))
    )
    val expected =
"""    SELECT
        col1
    FROM
        table1
    UNION ALL
    SELECT
        col2
    FROM
        table2
"""
    assertResult(expected) { Print.selectCore(input, 1) }
  }
  """
  SELECT
    x.col1 + x.col2 AS newX
  FROM
  (
    SELECT
      1 AS col1,
      2 AS col2
  ) x
  """ should "be printed as SELECT core with indent=2" in {
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
    val input = SqliteSelectCore(selectCols = cols, from = Some(from))
    val expected =
"""        SELECT
            x.col1 + x.col2 AS newX
        FROM
            (
                SELECT
                    1 AS col1,
                    2 AS col2
            ) AS x
"""
    assertResult(expected) { Print.selectCore(input, 2) }
  }
}

class PrintCteExprTests extends UnitSpec {
  "tmp1 AS (SELECT 1 AS A)" should "be printed as simple single CTE" in {
    val sel = SqliteSelectCore(
      selectCols = SqliteSelectColumns(
        cols = Seq(
          SqliteResultCol(colExpr = Some(SqliteIntegerLit(1)), colAlias = Some("A"))
        )
      )
    )
    val input = SqliteCommonTableExpr(cteName = "tmp1", cteBody = sel)
    val expected =
"""    tmp1 AS (
        SELECT
            1 AS A
    )"""
    assertResult(expected) { Print.cte(input, 1) }
  }
  "WITH tmp1 AS (SELECT 1 AS A), tmp2  AS (SELECT 2 AS B)" should "be printed as two CTE without indent" in {
    val tmp1 = SqliteSelectCore(
      selectCols = SqliteSelectColumns(
        cols = Seq(SqliteResultCol(colExpr = Some(SqliteIntegerLit(1)), colAlias = Some("A")))
      )
    )
    val tmp2 = SqliteSelectCore(
      selectCols = SqliteSelectColumns(
        cols = Seq(SqliteResultCol(colExpr = Some(SqliteIntegerLit(2)), colAlias = Some("B")))
      )
    )
    val input = Seq(
      SqliteCommonTableExpr(
        cteName = "tmp1",
        cteBody = tmp1,
      ),
      SqliteCommonTableExpr(
        cteName = "tmp2",
        cteBody = tmp2,
      ),
    )
    val expected =
"""WITH tmp1 AS (
    SELECT
        1 AS A
),
tmp2 AS (
    SELECT
        2 AS B
)"""
    assertResult(expected) { Print.manyCtes(input, 0) }
  }
}

class PrintSelectTests extends UnitSpec {
  "SELECT a, b FROM tableA order BY b DESC LIMIT 10" should "be printed as select with order and limit" in {
    val order = SqliteOrderByExpr(
      orderingTerms = Seq(
        SqliteOrderingTerm(expr = SqliteColumnExpr(columnName = "b"), ascending = false)
      )
    )
    val limit = SqliteLimitExpr(limitExpr = SqliteIntegerLit(10))
    val input = SqliteSelect(
      mainSelect = SqliteSelectCore(
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
      ),
      orderBy = Some(order),
      limit = Some(limit),
    )
    val expected =
"""    SELECT
        a,
        b
    FROM
        tableA
    ORDER BY
        b DESC
    LIMIT
        10
"""
    assertResult(expected) { Print.select(input, 1) }
  }
}
