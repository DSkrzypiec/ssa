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


// Keywords
sealed trait SqliteKeyword extends SqliteToken
case object SELECT extends SqliteKeyword
case object CURRENT extends SqliteKeyword

