package dev.dskrzypiec.parser.sqlite

import fastparse._, NoWhitespace._
import dev.dskrzypiec.parser.Common._
import dev.dskrzypiec.parser.sqlite.Identifier.id

object Expr {
  // TODO: Unary operators!
  // TODO: Parser for parsing Sqlite expression.
  def expr[_ : P]: P[SqliteExpr] = P(BinaryOp.orOp)

  def exprSingle[_ : P]: P[SqliteExpr] = P(FuncCallExpr.funcExpr | Column.columnExpr | Literal.literal | CaseExpr.caseExpr)
  def exprInParen[_ : P]: P[SqliteExpr] = P("(" ~/ expr ~ ")")
  def exprSingleOrParen[_ : P]: P[SqliteExpr] = P(exprSingle | exprInParen)

  // Binary operators and its precedence
  object BinaryOp {
    def concat[_ : P]: P[SqliteExpr] =
      P(exprSingleOrParen ~ ws ~ (StringIn("||", "->", "->>").! ~ ws ~ exprSingleOrParen ~ ws).rep).map(x => binaryOpTreeBuilder(x._1, x._2))

    def divMul[_ : P]: P[SqliteExpr] =
      P(concat ~ ws ~ (CharIn("*/%").! ~ ws ~ concat ~ ws).rep).map(x => binaryOpTreeBuilder(x._1, x._2))

    def addSub[_ : P]: P[SqliteExpr] =
      P(divMul ~ ws ~ (CharIn("+\\-").! ~ ws ~ divMul).rep).map(e => binaryOpTreeBuilder(e._1, e._2))

    def shift[_ : P]: P[SqliteExpr] =
      P(addSub ~ ws ~ (StringIn("&", "|", "<<", ">>").! ~ ws ~ addSub).rep).map(e => binaryOpTreeBuilder(e._1, e._2))

    def ineq[_ : P]: P[SqliteExpr] =
      P(shift ~ ws ~ (StringIn("<", "<=", ">", ">=").! ~ ws ~ shift).rep).map(e => binaryOpTreeBuilder(e._1, e._2))

    def comp[_ : P]: P[SqliteExpr] =
      P(ineq ~ ws ~ (StringIn("=", "<>", "!=" /* TODO: IS, IS NOT, BETWEEN, etc. */).! ~ ws ~ ineq).rep).map(e => binaryOpTreeBuilder(e._1, e._2))

   def andOp[_ : P]: P[SqliteExpr] =
     P(comp ~ ws ~ (icWord("and").! ~ ws ~ comp).rep).map(e => binaryOpTreeBuilder(e._1, e._2))

   def orOp[_ : P]: P[SqliteExpr] =
     P(andOp ~ ws ~ (icWord("or").! ~ ws ~ andOp).rep).map(e => binaryOpTreeBuilder(e._1, e._2))

    def binaryOpTreeBuilder(prev: SqliteExpr, ss: Seq[(String, SqliteExpr)]): SqliteExpr = {
      ss match {
        case Seq() => prev
        case Seq((op, expr)) => SqliteBinaryOp(charToBinaryOpSign(op), prev, expr)
        case Seq((headOp, headExpr), tail @ _*) => {
          SqliteBinaryOp(charToBinaryOpSign(headOp), prev, binaryOpTreeBuilder(headExpr, tail))
        }
      }
    }

    def charToBinaryOpSign(c: String): SqliteBinaryOpSign = c match {
      case "+" => ADD
      case "-" => SUB
      case "*" => MUL
      case "/" => DIV
      case "%" => MOD
      case "||" => CONCAT
      case "->" => ARROW
      case "->>" => DOUBLE_ARROW
      case "&" => BITAND
      case "|" => BITOR
      case "<<" => LSHIFT
      case ">>" => RSHIFT
      case "<" => LESS_THEN
      case ">" => GREATER_THEN
      case "<=" => LESS_OR_EQ
      case ">=" => GREATER_OR_EQ
      case "=" => EQUAL
      case "==" => EQUAL
      case "!=" => NOT_EQUAL
      case "<>" => NOT_EQUAL
      case c if (c.toLowerCase() == "and") => AND
      case c if (c.toLowerCase() == "or") => OR
      case _ => UNKNOWN_BINOP
    }
  }

  // f(...)
  object FuncCallExpr {
    def funcExpr[_ : P]: P[SqliteFuncCall] =
      // TODO: Should also handle "*", "DISTINCT" and FILTER and OVER clauses
      P(id ~ openParen ~ (expr ~ ws ~ comma.?).rep(1) ~ closeParen).map(e => SqliteFuncCall(e._1, e._2.toList))
  }

  // CASE ... WHEN ... THEN ... ELSE ... END
  object CaseExpr {
    def caseExpr[_ : P]: P[SqliteCaseExpr] = {
      P(caseInitExpr ~ ws ~ caseWhenThens ~ ws ~ caseElse ~ ws ~ caseEnd).map(
        e => SqliteCaseExpr(e._1, e._2.toList, e._3)
      )
    }

    def caseInitExpr[_: P]: P[Option[SqliteExpr]] =
      P(IgnoreCase("CASE") ~ ws ~ expr.?)

    def caseWhenThens[_ : P]: P[Seq[SqliteCaseWhenThen]] =
      P(IgnoreCase("WHEN") ~ ws ~ expr ~ ws ~ IgnoreCase("THEN") ~ ws ~ expr).rep(1).map (
        e => e.map(p => SqliteCaseWhenThen(p._1, p._2))
      )

    def caseElse[_ : P]: P[Option[SqliteExpr]] = P(IgnoreCase("ELSE") ~ ws ~ expr).?
    def caseEnd[_ : P] = P(IgnoreCase("END"))
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
      P(id ~ !openParen).map(e => SqliteColumnExpr(columnName = e))
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
