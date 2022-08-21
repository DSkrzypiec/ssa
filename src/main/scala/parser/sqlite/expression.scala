package dev.dskrzypiec.parser.sqlite

import fastparse._, NoWhitespace._
import dev.dskrzypiec.parser.Common._
import dev.dskrzypiec.parser.sqlite.Identifier.id

object Expr {
  def expr[_ : P]: P[SqliteExpr] = {
    P(
      Column.columnExpr // | Literal | ...
    )
  }

  // schema-name.?table-name.?column-name
  object Column {
    // schema.table.column or table.column or column identifiers
    def columnExpr[_ : P]: P[SqliteColumnExpr] = {
      P(columnFullExpr | columnSchema | columnNameOnly)
    }

    def columnFullExpr[_ : P]: P[SqliteColumnExpr] = {
      P(id.! ~ dot ~ id.! ~ dot ~ id.!).map {
        e => SqliteColumnExpr(Some(e._1), Some(e._2), e._3)
      }
    }

    def columnSchema[_ : P]: P[SqliteColumnExpr] = {
      P(id.! ~ dot ~ id.!).map {
        e => SqliteColumnExpr(tableName = Some(e._1), columnName = e._2)
      }
    }

    def columnNameOnly[_ : P]: P[SqliteColumnExpr] = {
      P(id).map(e => SqliteColumnExpr(columnName = e))
    }
  }

  object Literal {
    // Literal in Sqlite is one of the following:
    //  - numeric literal
    //  - string literal
    //  - blob literal
    //  - NULL
    //  - TRUE
    //  - FALSE
    //  - CURRENT_DATE
    //  - CURRENT_TIME
    //  - CURRENT_TIMESTAMP
    def literal[_ : P]: P[SqliteLiteral] = {
      P(
        nullLit |
        trueLit |
        falseLit |
        currentDateLit |
        currentTimeLit |
        currentTimestampLit |
        string |
        hex |
        integer |
        decimalSci |
        decimalFromDot |
        decimalSimple |
        literalErr
      )
    }

    // 1.223E+8
    def decimalSci[_ : P]: P[SqliteDoubleLit] = {
      P(digit.rep(1) ~ dot.? ~ digit.rep ~
        IgnoreCase("e") ~
        ("+" | "-").? ~ digit.rep).!.map {
          num => SqliteDoubleLit(num.toDouble)
        }
    }
    // 123123.1233
    def decimalSimple[_ : P]: P[SqliteDoubleLit] = {
      P(digit.rep(1) ~ dot.? ~ digit.rep).!.map {
        num => SqliteDoubleLit(num.toDouble)
      }
    }

    // .334
    def decimalFromDot[_ : P]: P[SqliteDoubleLit] = {
      P(dot ~ digit.rep(1)).!.map {
        num => SqliteDoubleLit(num.toDouble)
      }
    }

    // 0x123FA3
    def hex[_ : P]: P[SqliteHexLit] = {
      P(hexPrefix ~ hexChar.rep.!).map {
        e => SqliteHexLit("0x" + e, Integer.parseInt(e, 16))
      }
    }

    // TODO: Properly handle string parsing, including 'ex''escp'.
    def string[_ : P]: P[SqliteStringLit] = {
      P("'" ~ (digit | lowAz | upAz | underscore | dot | dollar ).rep.! ~ "'").map {
        s => SqliteStringLit(s)
      }
    }

    def integer[_ : P]: P[SqliteIntegerLit] = {
      P(digit.rep(1) ~ !(dot | IgnoreCase("e"))).!.map {
        e => SqliteIntegerLit(e.toInt)
      }
    }

    def currentDateLit[_ : P]: P[SqliteLiteral] = P(IgnoreCase("current_date") ~ peekWs).map {
      _ => SqliteCurrentDate()
    }
    def currentTimeLit[_ : P]: P[SqliteLiteral] = P(IgnoreCase("current_time") ~ peekWs).map {
      _ => SqliteCurrentTime()
    }
    def currentTimestampLit[_ : P]: P[SqliteLiteral] = P(IgnoreCase("current_timestamp") ~ peekWs).map {
      _ => SqliteCurrentTimestamp()
    }
    def trueLit[_ : P]: P[SqliteTrue] = P(IgnoreCase("true") ~ peekWs).map(_ => SqliteTrue())
    def falseLit[_ : P]: P[SqliteFalse] = P(IgnoreCase("false") ~ peekWs).map(_ => SqliteFalse())
    def nullLit[_ : P]: P[SqliteNull] = P(IgnoreCase("null") ~ peekWs).map(_ => SqliteNull())

    def literalErr[_ : P]: P[SqliteLiteralError] = P(Fail)
  }
}
