package dev.dskrzypiec.parser.sqlite

sealed trait SqliteToken

// Expression - https://www.sqlite.org/lang_expr.html
sealed trait SqliteExpr extends SqliteToken

sealed trait SqliteLiteralValue extends SqliteExpr
case class SqliteNumericLit(value: Double) extends SqliteLiteralValue
case class SqliteStringLit(value: String) extends SqliteLiteralValue
case class SqliteBlobLit(value: String) extends SqliteLiteralValue
case object SqliteNull extends SqliteLiteralValue
case object SqliteTrue extends SqliteLiteralValue
case object SqliteFalse extends SqliteLiteralValue
case object SqliteCurrentTime extends SqliteLiteralValue
case object SqliteCurrentDate extends SqliteLiteralValue
case object SqliteCurrentTimestamp extends SqliteLiteralValue




