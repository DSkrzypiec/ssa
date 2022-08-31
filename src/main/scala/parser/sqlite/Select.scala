package dev.dskrzypiec.parser.sqlite

import fastparse._, NoWhitespace._
import dev.dskrzypiec.parser.Common._
import dev.dskrzypiec.parser.sqlite.Expr.expr
import dev.dskrzypiec.parser.sqlite.Identifier.id

object SelectStmt {
  // Core part of SELECT statement
  // def selectCore[_ : P]: P[SqliteSelectComponent] = P()

  object TableOrSub {
    // def tableOrSubquery[_ : P]: P[SqliteSelectComponent] = 
    //
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
