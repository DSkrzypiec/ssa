package dev.dskrzypiec.parser.sqlite

import fastparse._, NoWhitespace._
import dev.dskrzypiec.parser.Common._
import dev.dskrzypiec.parser.sqlite.Expr.expr
import dev.dskrzypiec.parser.sqlite.Identifier.id

object SelectStmt {
  // Core part of SELECT statement
  object SelectCore {
    def selectCore[_ : P]: P[SqliteSelectCore] =
      P(
        icWord("select") ~ ws ~
        selectColumns ~ ws ~
        selectFrom ~
        (ws ~ whereExpr ~ ws).? ~
        (ws ~ groupByExpr ~ ws).? ~
        (ws ~ havingExpr ~ ws).?
        ).map(e => SqliteSelectCore(e._1, e._2, e._3, e._4))

    def selectColumns[_ : P]: P[SqliteSelectColumns] =
      P((icWord("distinct") | icWord("all")).!.? ~ ws ~ ResultCol.resultCols).map(e => e._1 match {
          case None => SqliteSelectColumns(cols = e._2)
          case Some(w) if w.toLowerCase() == "distinct" => SqliteSelectColumns(distinct = true, cols = e._2)
          case Some(w) if w.toLowerCase() == "all" => SqliteSelectColumns(all = true, cols = e._2)
          case Some(_) => SqliteSelectColumns(cols = e._2) // should not happen
        })

    def selectFrom[_ : P]: P[SqliteSelectFrom] =
      P(icWord("from") ~ ws ~ (TableOrSub.tableOrSubquery | Joins.joinExpr)).map(e => e match {
        case table: SqliteTableName => SqliteSelectFrom(table = Some(table))
        case joinExpr: SqliteJoinExpr => SqliteSelectFrom(joinExpr = Some(joinExpr))
        case _ => SqliteSelectFrom()
      })
  }

  object Joins {
    // Parser for JOIN expression. Usually like table -> JOIN op -> table ->
    // JOIN constraint with possibly many joins repeated.
    def joinExpr[_ : P]: P[SqliteJoinExpr] = P(
      TableOrSub.tableOrSubquery ~ ws ~
      (
        joinOperator ~ ws ~
        TableOrSub.tableOrSubquery ~ ws ~
        joinConstrain
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
      P(icWord("natural").? ~ ws ~ icWord("inner") ~ ws ~ icWord("join")).map(_ => SqliteJoinInner())

    def joinCross[_ : P]: P[SqliteJoinOperator] =
      P(icWord("cross") ~ ws ~ icWord("join")).map(_ => SqliteJoinCross())

    def joinComma[_ : P]: P[SqliteJoinOperator] =
      P(comma).map(_ => SqliteJoinInner(usingCommas = true))
  }

  object TableOrSub {
    // TODO: the following is not the full definition
    def tableOrSubquery[_ : P]: P[SqliteTableName] = tableName

    def tableName[_ : P]: P[SqliteTableName] =
      P((id.! ~ dot).? ~ id.! ~ ws ~ icWord("as").? ~ ws ~ (id.!).?).map(
        e => SqliteTableName(schemaName = e._1, tableName = e._2, tableAlias = e._3)
      )
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
}
