package dev.dskrzypiec.parser.sqlite

object Print {
  private final val SingleSpace = " "
  private final val NewLine = sys.props("line.separator")

  def sqliteSelect(q: SqliteSelect): String = {
    "TODO"
  }

  def cte(q: SqliteCommonTableExpr): String = {
    "TODO"
  }

  def selectCore(q: SqliteSelectCore, indent: Int = 0): String = {
    indLvl(indent) + "SELECT" + NewLine +
    selectCols(q.selectCols, indent + 1) + NewLine +
    stringWithNewLineOrEmpty(q.from, selectFrom, indent) +
    stringWithNewLineOrEmpty(q.where, whereExpr, indent) +
    stringWithNewLineOrEmpty(q.groupBy, groupByExpr, indent) +
    stringWithNewLineOrEmpty(q.having, havingExpr, indent) +
    stringWithNewLineOrEmpty(q.setOp, setOpExpr, indent, skipNewLine = true)
  }

  def selectCols(cols: SqliteSelectColumns, indent: Int = 0): String = {
    {
      cols.distinct match {
        case true => indLvl(indent) + "DISTINCT" + NewLine
        case false => ""
      }
    } +
    {
      cols.all match {
        case true => indLvl(indent) + "ALL" + NewLine
        case false => ""
      }
    } +
    cols.cols.map(c => selectResultCol(c, indent)).mkString("," + NewLine)
  }

  def selectResultCol(col: SqliteResultCol, indent: Int = 0): String = {
    if (col.isStar) {
      return indLvl(indent) + "*"
    }
    if (col.isStarInTableName.isDefined) {
      return indLvl(indent) + s"${col.isStarInTableName.get}.*"
    }
    val sb = new StringBuilder()
    if (col.colExpr.isDefined) {
      sb.append(indLvl(indent) + sqliteExpr(col.colExpr.get))
    }
    if (col.colAlias.isDefined) {
      sb.append(SingleSpace + "AS" + SingleSpace + col.colAlias.get)
    }

    sb.toString
  }

  def selectFrom(from: SqliteSelectFrom, indent: Int = 0): String = {
    indLvl(indent) + "FROM" + NewLine +
    {
      from.tableOrSubquery match {
        case None => ""
        case Some(tos) => tableOrSubquery(tos, indent + 1)
      }
    } +
    {
      from.joinExpr match {
        case None => ""
        case Some(je) => joinExpr(je, indent)
      }
    }
  }

  def joinExpr(query: SqliteJoinExpr, indent: Int = 0): String = {
    indLvl(indent + 1) + tableOrSubquery(query.firstTable) +
    query.otherJoins.map(joinSingleExpr(_, indent)).mkString("")
  }

  def joinSingleExpr(t: (SqliteJoinOperator, SqliteTableOrSubquery, SqliteJoinConstraint), indent: Int = 0): String = {
    NewLine +
    indLvl(indent) + joinOpToString(t._1) + NewLine +
    indLvl(indent + 1) + tableOrSubquery(t._2) + SingleSpace + "ON" + SingleSpace +
    (
      t._3.joinExpression match {
        case None => "!PRINTING JOINING BY COLUMNS IN NOT YET SUPPORTED!" // TODO
        case Some(expr) => sqliteExpr(expr)
      }
    )
  }

  def tableOrSubquery(query: SqliteTableOrSubquery, indent: Int = 0): String = {
    if (query.table.isDefined) {
      return tableName(query.table.get, indent)
    }
    if (query.subQuery.isDefined) {
      indLvl(indent) + "(" + NewLine +
      selectCore(query.subQuery.get.subQuery, indent + 1) +
      indLvl(indent) + ")" + (query.subQuery.get.alias match {
        case None => ""
        case Some(alias) => SingleSpace + "AS" + SingleSpace + query.subQuery.get.alias.get
      })
    } else {
      "!EVEN TABLE OR SUBQUERY IS NOT DEFINED!"
    }
  }

  def tableName(t: SqliteTableName, indent: Int = 0): String = {
    indLvl(indent) + (t.schemaName match {
      case None => ""
      case Some(name) => name + "."
    }) + t.tableName + (t.tableAlias match {
      case None => ""
      case Some(aliasName) => SingleSpace + "AS" + SingleSpace + aliasName
    })
  }

  def whereExpr(query: SqliteWhereExpr, indent: Int = 0): String = {
    indLvl(indent) + "WHERE" + NewLine +
    indLvl(indent + 1) + sqliteExpr(query.condition)
  }

  def groupByExpr(query: SqliteGroupByExpr, indent: Int = 0): String = {
    indLvl(indent) + "GROUP BY" + NewLine +
    query.groupingExprs.map(indLvl(indent + 1) + sqliteExpr(_)).mkString("," + NewLine)
  }

  def havingExpr(query: SqliteHavingExpr, indent: Int = 0): String = {
    indLvl(indent) + "HAVING" + NewLine +
    indLvl(indent + 1) + sqliteExpr(query.condition)
  }

  def limitExpr(query: SqliteLimitExpr, indent: Int = 0): String = {
    indLvl(indent) + "LIMIT" + NewLine +
    indLvl(indent + 1) + sqliteExpr(query.limitExpr) +
    {
      query.offsetExpr match {
        case None => ""
        case Some(e) => SingleSpace + "OFFSET" + SingleSpace + sqliteExpr(e)
      }
    }
  }

  def orderByExpr(query: SqliteOrderByExpr, indent: Int = 0): String = {
    indLvl(indent) + "ORDER BY" + NewLine +
    query.orderingTerms.map(indLvl(indent + 1) + orderingTerm(_)).mkString("," + NewLine)
  }

  def orderingTerm(ot: SqliteOrderingTerm, indent: Int = 0): String = {
    val sb = new StringBuilder()
    sb.append(indLvl(indent))
    sb.append(sqliteExpr(ot.expr))
    if (ot.collationName.isDefined) {
      sb.append(SingleSpace + "COLLATE" + SingleSpace + ot.collationName.get)
    }
    if (!ot.ascending) {
      sb.append(SingleSpace + "DESC")
    }
    if (!ot.nullsLast) {
      sb.append(SingleSpace + "NULLS FIRST")
    }

    sb.toString
  }

  def setOpExpr(q: SqliteSetExpr, indent: Int = 0): String = {
    indLvl(indent) + setOperatorToString(q.setOp) + NewLine +
    selectCore(q.anotherSelect, indent)
  }

  def sqliteExpr(expr: SqliteExpr): String = {
    expr match {
      case l: SqliteLiteral => literal(l)
      case cr: SqliteColumnExpr => columnExpr(cr)
      case ce: SqliteCaseExpr => caseExpr(ce)
      case fc: SqliteFuncCall => funcCall(fc)
      case binOp: SqliteBinaryOp => binaryOp(binOp)
    }
  }

  def binaryOp(binOp: SqliteBinaryOp): String = {
    sqliteExpr(binOp.left) + SingleSpace +
    binaryOpSignToString(binOp.op) + SingleSpace +
    sqliteExpr(binOp.right)
  }

  def funcCall(expr: SqliteFuncCall): String = {
    (expr.starAsArg, expr.distinctArgs) match {
      case (false, false) => s"${expr.func}(${expr.args.map(sqliteExpr(_)).mkString(", ")})"
      case (true, false) => s"${expr.func}(*)"
      case (false, true) => s"${expr.func}(DISTINCT ${expr.args.map(sqliteExpr(_)).mkString(", ")})"
      case (true, true) => s"${expr.func}(DISTINCT *)"
    }
  }

  def caseExpr(expr: SqliteCaseExpr, indent: Int = 0): String = {
    indLvl(indent) + "CASE" + NewLine +
    {
      expr.initalExpr match {
        case None => ""
        case Some(e) => indLvl(indent + 1) + sqliteExpr(e) + NewLine
      }
    } +
    expr.whenThens.map(e => caseWhenThenExpr(e, indent + 1)).mkString(NewLine) + NewLine +
    {
      expr.elseExpr match {
        case None => ""
        case Some(e) => indLvl(indent + 1) + "ELSE" + SingleSpace + sqliteExpr(e) + NewLine
      }
    } +
    indLvl(indent) + "END"
  }

  def caseWhenThenExpr(expr: SqliteCaseWhenThen, indent: Int = 0): String = {
    indLvl(indent) + "WHEN" + SingleSpace + sqliteExpr(expr.when) + SingleSpace +
    "THEN" + SingleSpace + sqliteExpr(expr.then)
  }

  def columnExpr(expr: SqliteColumnExpr): String = {
    (expr.schemaName, expr.tableName) match {
      case (None, None) => expr.columnName
      case (None, Some(t)) => s"${t}.${expr.columnName}"
      case (Some(s), Some(t)) => s"${s}.${t}.${expr.columnName}"
      case (Some(s), None) => s"${s}.MISSING!.${expr.columnName}"
    }
  }

  def literal(lit: SqliteLiteral): String = {
    lit match {
      case SqliteIntegerLit(n) => n.toString()
      case SqliteDoubleLit(d) => d.toString()
      case SqliteHexLit(input, _) => input
      case SqliteStringLit(s) => s"'${s}'"
      case SqliteBlobLit(b) => s"'${b}'"
      case SqliteNull() => "NULL"
      case SqliteTrue() => "TRUE"
      case SqliteFalse() => "FALSE"
      case SqliteCurrentTimestamp() => "CURRENT_TIMESTAMP"
      case SqliteCurrentTime() => "CURRENT_TIME"
      case SqliteCurrentDate() => "CURRENT_DATE"
      case SqliteLiteralError() => "_LITERAL_ERROR!"
    }
  }

  def setOperatorToString(op: SqliteSetOperator): String = {
    op match {
      case SqliteUnion() => "UNION"
      case SqliteUnionAll() => "UNION ALL"
      case SqliteIntersect() => "INTERSECT"
      case SqliteExcept() => "EXCEPT"
    }
  }

  def joinOpToString(op: SqliteJoinOperator): String = {
    op match {
      case SqliteJoinLeft() => "LEFT JOIN"
      case SqliteJoinRight() => "RIGHT JOIN"
      case SqliteJoinFull() => "FULL JOIN"
      case SqliteJoinCross() => "CROSS JOIN"
      case SqliteJoinInner(_) => "INNER JOIN"
    }
  }

  def binaryOpSignToString(op: SqliteBinaryOpSign): String = {
    op match {
      case ADD => "+"
      case SUB => "-"
      case MUL => "*"
      case DIV => "/"
      case MOD => "%"
      case CONCAT => "||"
      case ARROW => "->"
      case DOUBLE_ARROW => "->>"
      case BITAND => "&"
      case BITOR => "|"
      case LSHIFT => "<<"
      case RSHIFT => ">>"
      case LESS_THEN => "<"
      case GREATER_THEN => ">"
      case LESS_OR_EQ => "<="
      case GREATER_OR_EQ => ">="
      case EQUAL => "="
      case NOT_EQUAL => "!="
      case AND => "AND"
      case OR => "OR"
      case UNKNOWN_BINOP => "UNKNOWN"
    }
  }

  def indLvl(level: Int = 0, width: Int = 4): String = {
    SingleSpace * level * width
  }

  def getStringOrEmpty(str: Option[String], indent: Int = 0): String = {
    str match {
      case Some(s) => indLvl(indent) + s
      case None => ""
    }
  }

  def stringWithNewLineOrEmpty[T](
    t: Option[T], f: (T, Int) => String, indent: Int = 0,
    skipNewLine: Boolean = false): String = {
    (t, skipNewLine) match {
      case (None, _) => ""
      case (Some(obj), false) => f(obj, indent) + NewLine
      case (Some(obj), true) => f(obj, indent)
    }
  }
}

