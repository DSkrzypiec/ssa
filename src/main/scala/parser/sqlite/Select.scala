package dev.dskrzypiec.parser.sqlite

import fastparse._, NoWhitespace._
import dev.dskrzypiec.parser.Common._
import dev.dskrzypiec.parser.sqlite.Expr.expr
import dev.dskrzypiec.parser.sqlite.Identifier.id

object SelectStmt {
  object Select {
    def select[_ : P]: P[SqliteSelect] = {
      P(
        (ws.? ~ manyCtes).? ~
        (ws.? ~ SelectCore.selectCore /* | TODO(values) */) ~
        (ws ~ OrderBy.orderByExpr).? ~
        (ws ~ Limit.limitExpr).?
       ).map(e => e._1 match {
         case None => SqliteSelect( // no CTEs
           mainSelect = e._2,
           orderBy = e._3,
           limit = e._4
         )
          case Some(cte) => SqliteSelect( // with CTEs
            ctesRecursive = cte._1,
            ctes = Some(cte._2),
            mainSelect = e._2,
            orderBy = e._3,
            limit = e._4
          )
       })
    }

    def manyCtes[_ : P]: P[(Boolean, Seq[SqliteCommonTableExpr])] =
      P(
        icWord("with") ~ ws ~ icWord("recursive").!.? ~ ws ~
        (ws ~ CTE.singleCTE ~ ws).rep(sep = ",")
      ).map(e => e._1 match {
        case None => (false, e._2)
        case Some(_) => (true, e._2)
      })
  }

  object CTE {
    def singleCTE[_ : P]: P[SqliteCommonTableExpr] =
      P(
        id.! ~ ws.? ~
        (openParen ~ (ws.? ~ id.! ~ ws.?).rep(sep=",") ~ ws.? ~ closeParen).? ~ ws ~
        icWord("as") ~ ws ~
        (
          (icWord("not").! ~ ws ~ icWord("materialized")) |
          (icWord("materialized").!)
        ).? ~ ws ~
        openParen ~ ws ~ SelectCore.selectCore ~ ws ~ closeParen
      ).map(e =>
        e._3 match {
          case None => SqliteCommonTableExpr(
            cteName = e._1,
            cteColNames = e._2,
            cteBody = e._4
          )
          case Some(str) if str.toLowerCase() == "materialized" => SqliteCommonTableExpr(
              cteName = e._1,
              cteColNames = e._2,
              isMaterialized = true,
              cteBody = e._4
            )
          case _ => SqliteCommonTableExpr(
              cteName = e._1,
              cteColNames = e._2,
              isMaterialized = false,
              cteBody = e._4
            )
        }
      )
  }

  object SelectCore {
    def selectCore[_ : P]: P[SqliteSelectCore] =
      P(
        icWord("select") ~ ws ~
        selectColumns ~
        (ws ~ selectFrom ~ ws).? ~
        (ws ~ whereExpr ~ ws).? ~
        (ws ~ groupByExpr ~ ws).? ~
        (ws ~ havingExpr ~ ws).? ~
        (ws ~ selectSetOp ~ ws).?
        ).map(e => SqliteSelectCore(e._1, e._2, e._3, e._4, e._5, e._6))

    def selectColumns[_ : P]: P[SqliteSelectColumns] =
      P((icWord("distinct") | icWord("all")).!.? ~ ws ~ ResultCol.resultCols).map(e => e._1 match {
          case None => SqliteSelectColumns(cols = e._2)
          case Some(w) if w.toLowerCase() == "distinct" => SqliteSelectColumns(distinct = true, cols = e._2)
          case Some(w) if w.toLowerCase() == "all" => SqliteSelectColumns(all = true, cols = e._2)
          case Some(_) => SqliteSelectColumns(cols = e._2) // should not happen
        })

    def selectFrom[_ : P]: P[SqliteSelectFrom] =
      P(icWord("from") ~ ws ~ (Joins.joinExpr | TableOrSub.tableOrSubquery)).map(e => e match {
        case table: SqliteTableOrSubquery => SqliteSelectFrom(tableOrSubquery = Some(table))
        case joinExpr: SqliteJoinExpr => SqliteSelectFrom(joinExpr = Some(joinExpr))
        case _ => SqliteSelectFrom()
      })

    def selectSetOp[_ : P]: P[SqliteSetExpr] =
      P(SetOp.setOperator ~ ws ~ selectCore).map(e => SqliteSetExpr(e._1, e._2))
  }

  object Joins {
    // Parser for JOIN expression. Usually like table -> JOIN op -> table ->
    // JOIN constraint with possibly many joins repeated.
    def joinExpr[_ : P]: P[SqliteJoinExpr] = P(
      TableOrSub.tableOrSubquery ~
      (
        ws.? ~ joinOperator ~ ws ~
        TableOrSub.tableOrSubquery ~ ws ~
        joinConstrain ~ ws.?
      ).rep(1)
    ).map(e => SqliteJoinExpr(firstTable = e._1, otherJoins = e._2))

    // Parsing join constrain: either ON [expr] or USING (col1, col2, ...).
    def joinConstrain[_ : P]: P[SqliteJoinConstraint] =
      P(joinExpression | joinByColumns).map(
        e => e match {
          case expr: SqliteExpr => SqliteJoinConstraint(joinExpression = Some(expr))
          case cols: Seq[String] => SqliteJoinConstraint(byColumnNames = cols.toList)
        }
      )

    def joinExpression[_ : P]: P[SqliteExpr] = P(icWord("on") ~ ws ~ expr)

    def joinByColumns[_ : P]: P[Seq[String]] =
      P(icWord("using") ~ ws ~ openParen ~ (ws ~ id.! ~ ws).rep(sep=",") ~ closeParen)

    // [NATURAL] (LEFT | RIGHT | FULL) [OUTER] JOIN
    def joinOperator[_ : P]: P[SqliteJoinOperator] = P(joinComma | joinCross | joinInnerOp | joinOuterOp)

    def joinOuterOp[_ : P]: P[SqliteJoinOperator] = {
      P(
        (icWord("natural") ~ ws).? ~
        (icWord("left") | icWord("right") | icWord("full")).! ~ ws ~
        icWord("outer").? ~ ws ~ icWord("join")
      ).map(joinType =>
        joinType.toLowerCase() match {
          case "left" => SqliteJoinLeft()
          case "right" => SqliteJoinRight()
          case "full" => SqliteJoinFull()
        }
      )
    }

    def joinInnerOp[_ : P]: P[SqliteJoinOperator] =
      P((icWord("natural") ~ ws).? ~ icWord("inner") ~ ws ~ icWord("join")).map(_ => SqliteJoinInner())

    def joinCross[_ : P]: P[SqliteJoinOperator] =
      P(icWord("cross") ~ ws ~ icWord("join")).map(_ => SqliteJoinCross())

    def joinComma[_ : P]: P[SqliteJoinOperator] =
      P(comma).map(_ => SqliteJoinInner(usingCommas = true))
  }

  object TableOrSub {
    def tableOrSubquery[_ : P]: P[SqliteTableOrSubquery] = P(subQuery | tableName)

    def tableName[_ : P]: P[SqliteTableOrSubquery] =
      P((id.! ~ dot).? ~ id.! ~ ws ~ icWord("as").? ~ ws ~ (id.!).?).map(
        e => SqliteTableOrSubquery(table = Some(SqliteTableName(schemaName = e._1, tableName = e._2, tableAlias = e._3)))
      )

    def subQuery[_ : P]: P[SqliteTableOrSubquery] =
      P(
        openParen ~ ws ~ SelectCore.selectCore ~ ws ~ closeParen ~ ws ~
        icWord("as").? ~ ws ~ (id.!).?
      ).map(e => SqliteTableOrSubquery(subQuery = Some(SqliteSelectSubquery(subQuery = e._1, alias = e._2))))
  }

  object ResultCol {
    // Many result columns (usually in single SELECT)
    def resultCols[_ : P]: P[Seq[SqliteResultCol]] =
      P(ws ~ resultCol ~ ws).rep(sep=",")

    // Parsing single SELECT result column. It's usually an expression with
    // potential alias after "AS" token. But might be a "*" or table_name.*.
    def resultCol[_ : P]: P[SqliteResultCol] =
      P(resColStar | resTableStar | resExpr).map(e =>
        e match {
          case SqliteResultStar() => SqliteResultCol(isStar = true)
          case (ex: SqliteExpr, name: Option[String]) => SqliteResultCol(colExpr = Some(ex), colAlias = name)
          case tableName: String => SqliteResultCol(isStarInTableName = Some(tableName))
        }
      )

    def resExpr[_ : P]: P[(SqliteExpr, Option[String])] =
      P(expr ~ ws ~ (icWord("as").? ~ ws ~ id.!).?)

    def resTableStar[_ : P]: P[String] =
      P(id.! ~ dot ~ star).map(e => e)

    def resColStar[_ : P]: P[SqliteResultStar] =
      star.map(_ => SqliteResultStar())
  }

  // WHERE expression parser
  def whereExpr[_ : P]: P[SqliteWhereExpr] = P(icWord("where") ~ ws ~ expr).map(SqliteWhereExpr(_))

  // GROUP BY expression parser
  def groupByExpr[_ : P]: P[SqliteGroupByExpr] =
    P(icWord("group") ~ ws ~ icWord("by") ~ (ws ~ expr ~ ws).rep(sep = ",")).map(SqliteGroupByExpr(_))

  // HAVING
  def havingExpr[_ : P]: P[SqliteHavingExpr] = P(icWord("having") ~ ws ~ expr).map(SqliteHavingExpr(_))

  // WINDOW
  // TODO..

  object SetOp {
    def setOperator[_ : P]: P[SqliteSetOperator] = P(unionAll | union | except | intersect)

    def unionAll[_ : P]: P[SqliteUnionAll] =
      P(icWord("union") ~ ws ~ icWord("all")).map(_ => SqliteUnionAll())

    def union[_ : P]: P[SqliteUnion] =
      P(icWord("union")).map(_ => SqliteUnion())

    def intersect[_ : P]: P[SqliteIntersect] =
      P(icWord("intersect")).map(_ => SqliteIntersect())

    def except[_ : P]: P[SqliteExcept] =
      P(icWord("except")).map(_ => SqliteExcept())
  }

  object OrderBy {
    def orderByExpr[_ : P]: P[SqliteOrderByExpr] = {
      P(
        icWord("order") ~ ws ~ icWord("by") ~
        (ws ~ orderingTerm ~ ws).rep(sep=",")
      ).map(e => SqliteOrderByExpr(e))
    }

    def orderingTerm[_ : P]: P[SqliteOrderingTerm] = {
      P(
        expr ~ ws ~
        (icWord("collate") ~ ws ~ id.!).? ~ ws ~
        (icWord("asc").! | icWord("desc").!).? ~ ws ~
        (
          (icWord("nulls") ~ ws ~ icWord("first").!) |
          (icWord("nulls") ~ ws ~ icWord("last").!)
        ).?
      ).map(e => {
        val isAsc = e._3 match {
          case None => true
          case Some(s) if s.toLowerCase() == "asc" => true
          case Some(s) if s.toLowerCase() == "desc" => false
          case _ => true
        }
        val nullsLast = e._4 match {
          case None => true
          case Some(s) => s.toLowerCase() == "last"
        }
        SqliteOrderingTerm(e._1, e._2, isAsc, nullsLast)
      })
    }
  }

  object Limit {
    def limitExpr[_ : P]: P[SqliteLimitExpr] =
      P(
        icWord("limit") ~ ws ~ expr ~
        (
          (ws ~ icWord("offset") ~ ws ~ expr) |
          (ws.? ~ comma ~ ws.? ~ expr)
        ).?
      ).map(e => SqliteLimitExpr(e._1, e._2))
  }
}
