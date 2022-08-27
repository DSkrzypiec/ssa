package dev.dskrzypiec.parser.sqlite

sealed trait SqliteToken

// Expression - https://www.sqlite.org/lang_expr.html
sealed trait SqliteExpr extends SqliteToken
case class SqliteId(value: String) extends SqliteToken

sealed trait SqliteLiteral extends SqliteExpr
case class SqliteIntegerLit(value: Int) extends SqliteLiteral
case class SqliteDoubleLit(value: Double) extends SqliteLiteral
case class SqliteHexLit(input: String, value: Int) extends SqliteLiteral
case class SqliteStringLit(value: String) extends SqliteLiteral
case class SqliteBlobLit(value: String) extends SqliteLiteral
case class SqliteNull() extends SqliteLiteral
case class SqliteTrue() extends SqliteLiteral
case class SqliteFalse() extends SqliteLiteral
case class SqliteCurrentTimestamp() extends SqliteLiteral
case class SqliteCurrentTime() extends SqliteLiteral
case class SqliteCurrentDate() extends SqliteLiteral
case class SqliteLiteralError() extends SqliteLiteral

case class SqliteColumnExpr(
  schemaName: Option[String] = None,
  tableName: Option[String] = None,
  columnName: String
) extends SqliteExpr

// CASE
case class SqliteCaseExpr(
  initalExpr: Option[SqliteExpr] = None,
  whenThens: List[SqliteCaseWhenThen],
  elseExpr: Option[SqliteExpr] = None
) extends SqliteExpr
case class SqliteCaseWhenThen(when: SqliteExpr, then: SqliteExpr) extends SqliteExpr

// Function call
case class SqliteFuncCall(
  func: String, //SqliteFunction,
  args: List[SqliteExpr],
  starAsArg: Boolean = false,
  distinctArgs: Boolean = false
) extends SqliteExpr

sealed trait SqliteFunction
case object COUNT extends SqliteFunction
case object MIN extends SqliteFunction
case object MAX extends SqliteFunction
case object SUM extends SqliteFunction
case object AVG extends SqliteFunction

// Binary expressions and operators
sealed trait SqliteBinaryOpExpr extends SqliteExpr
case class SqliteBinaryOp(
  op: SqliteBinaryOpSign,
  left: SqliteExpr,
  right: SqliteExpr,

) extends SqliteBinaryOpExpr


sealed trait SqliteBinaryOpSign
case object ADD extends SqliteBinaryOpSign
case object SUB extends SqliteBinaryOpSign
case object MUL extends SqliteBinaryOpSign
case object DIV extends SqliteBinaryOpSign
case object MOD extends SqliteBinaryOpSign
case object CONCAT extends SqliteBinaryOpSign
case object ARROW extends SqliteBinaryOpSign
case object DOUBLE_ARROW extends SqliteBinaryOpSign
case object BITAND extends SqliteBinaryOpSign
case object BITOR extends SqliteBinaryOpSign
case object LSHIFT extends SqliteBinaryOpSign
case object RSHIFT extends SqliteBinaryOpSign
case object LESS_THEN extends SqliteBinaryOpSign
case object GREATER_THEN extends SqliteBinaryOpSign
case object LESS_OR_EQ extends SqliteBinaryOpSign
case object GREATER_OR_EQ extends SqliteBinaryOpSign
case object EQUAL extends SqliteBinaryOpSign
case object NOT_EQUAL extends SqliteBinaryOpSign

case object UNKNOWN_BINOP extends SqliteBinaryOpSign


// Keywords
sealed trait SqliteKeyword extends SqliteToken
case object SELECT extends SqliteKeyword
case object CURRENT extends SqliteKeyword

