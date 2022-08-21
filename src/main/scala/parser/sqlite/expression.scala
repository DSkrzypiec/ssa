package dev.dskrzypiec.parser.sqlite

import fastparse._, NoWhitespace._
import dev.dskrzypiec.parser.Common._
import dev.dskrzypiec.parser.sqlite.Identifier.id

object Expr {
  // schema-name.?table-name.?column-name
  object Column {
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
    def decimal[_ : P]: P[Double] = P(decimalHex | decimalSci | decimalSimple)

    // 1.223E+8
    def decimalSci[_ : P]: P[Double] = {
      P(digit.rep ~ dot.? ~ digit.rep ~
        IgnoreCase("e") ~
        ("+" | "-").? ~ digit.rep).!.map(_.toDouble)
    }
    // 123123.1233
    def decimalSimple[_ : P]: P[Double] = {
      P(digit.rep ~ dot.? ~ digit.rep).!.map(_.toDouble)
    }
    // 0x123FA3
    def decimalHex[_ : P]: P[Double] = {
      P(hexPrefix ~ hexChar.rep.!).map(e => Integer.parseInt(e, 16).toDouble)
    }

    // TODO: Properly handle string parsing, including 'ex''escp'.
    def string[_ : P]: P[String] = P("'" ~ (digit | lowAz | upAz).rep.! ~ "'")

    def integer[_ : P]: P[Int] = {
      P(digit.rep(1) ~ !(dot | IgnoreCase("e"))).!.map(_.toInt)
    }

    def currentTime[_ : P] = P(IgnoreCase("current_time"))
    def currentDate[_ : P] = P(IgnoreCase("current_date"))
    def currentTimestamp[_ : P] = P(IgnoreCase("current_timestamp"))
    def trueLit[_ : P] = P(IgnoreCase("true"))
    def falseLit[_ : P] = P(IgnoreCase("false"))
    def nullLit[_ : P] = P(IgnoreCase("null"))
  }
}
