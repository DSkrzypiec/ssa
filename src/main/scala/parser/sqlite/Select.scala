package dev.dskrzypiec.parser.sqlite

import fastparse._, NoWhitespace._
import dev.dskrzypiec.parser.Common._
import dev.dskrzypiec.parser.sqlite.Expr.expr
import dev.dskrzypiec.parser.sqlite.Identifier.id

object SelectStmt {
  // Core part of SELECT statement
  // def selectCore[_ : P]: P[SqliteSelectComponent] = P()

  object Joins {
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
    def tableOrSubquery[_ : P]: P[SqliteSelectComponent] = tableName

    def tableName[_ : P]: P[SqliteTableName] =
      P((id.! ~ dot).? ~ id.! ~ ws ~ icWord("as").? ~ ws ~ (id.!).?).map(
        e => SqliteTableName(schemaName = e._1, tableName = e._2, tableAlias = e._3)
      )
  }

  object ResultCol {
    // Many result columns (usually in single SELECT)
    def resultCols[_ : P]: P[Seq[SqliteSelectComponent]] =
      P(ws ~ resultCol ~ ws).rep(sep=",")

    // Parsing single SELECT result column. It's usually an expression with
    // potential alias after "AS" token. But might be a "*" or table_name.*.
    def resultCol[_ : P]: P[SqliteSelectComponent] =
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
}
