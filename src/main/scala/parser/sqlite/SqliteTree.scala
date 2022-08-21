package dev.dskrzypiec.parser.sqlite

sealed trait SqliteToken

// Expression - https://www.sqlite.org/lang_expr.html
sealed trait SqliteExpr extends SqliteToken
case class SqliteId(value: String) extends SqliteToken

sealed trait SqliteLiteralValue extends SqliteExpr
case class SqliteNumericLit(value: Double) extends SqliteLiteralValue
case class SqliteStringLit(value: String) extends SqliteLiteralValue
case class SqliteBlobLit(value: String) extends SqliteLiteralValue
case object SqliteNull extends SqliteLiteralValue
case object SqliteTrue extends SqliteLiteralValue
case object SqliteFalse extends SqliteLiteralValue

case class SqliteColumnExpr(
  schemaName: Option[String] = None,
  tableName: Option[String] = None,
  columnName: String
) extends SqliteExpr


// Keywords
sealed trait SqliteKeyword extends SqliteToken
case object SELECT extends SqliteKeyword
case object CURRENT extends SqliteKeyword
case object CURRENT_DATE extends SqliteKeyword
case object CURRENT_TIME extends SqliteKeyword
case object CURRENCT_TIMESTAMP extends SqliteKeyword

