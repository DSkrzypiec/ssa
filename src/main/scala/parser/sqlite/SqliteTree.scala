package dev.dskrzypiec.parser.sqlite

sealed trait SqliteToken

// Expression - https://www.sqlite.org/lang_expr.html
sealed trait SqliteExpr extends SqliteToken
case class SqliteId(value: String) extends SqliteToken

// SELECT
sealed trait SqliteSelectComponent extends SqliteToken

case class SqliteSelect(
  ctes: Option[Seq[SqliteCommonTableExpr]] = None,
  ctesRecursive: Boolean = false,
  mainSelect: SqliteSelectCore,
  orderBy: Option[SqliteOrderByExpr] = None,
) extends SqliteToken

case class SqliteCommonTableExpr(
  cteName: String,
  cteColNames: Option[Seq[String]] = None,
  isMaterialized: Boolean = false,
  cteBody: SqliteSelectCore
) extends SqliteSelectComponent

case class SqliteSelectCore(
  selectCols: SqliteSelectColumns,
  from: Option[SqliteSelectFrom] = None,
  where: Option[SqliteWhereExpr] = None,
  groupBy: Option[SqliteGroupByExpr] = None,
  having: Option[SqliteHavingExpr] = None,
  // window: TODO
  setOp: Option[SqliteSetExpr] = None,
) extends SqliteSelectComponent

case class SqliteSelectColumns(
  distinct: Boolean = false,
  all: Boolean = false,
  cols: Seq[SqliteResultCol]
) extends SqliteSelectComponent

case class SqliteSelectFrom(
  tableOrSubquery: Option[SqliteTableOrSubquery] = None,
  joinExpr: Option[SqliteJoinExpr] = None
)

case class SqliteResultCol(
  colExpr: Option[SqliteExpr] = None,
  colAlias: Option[String] = None,
  isStar: Boolean = false,
  isStarInTableName: Option[String] = None
) extends SqliteSelectComponent

case class SqliteResultStar() extends SqliteSelectComponent

case class SqliteTableOrSubquery(
  table: Option[SqliteTableName] = None,
  subQuery: Option[SqliteSelectSubquery] = None
) extends SqliteSelectComponent

case class SqliteTableName(
  schemaName: Option[String] = None,
  tableName: String,
  tableAlias: Option[String] = None
) extends SqliteSelectComponent

case class SqliteSelectSubquery(
  subQuery: SqliteSelectCore,
  alias: Option[String] = None
) extends SqliteSelectComponent

case class SqliteJoinExpr(
  firstTable: SqliteTableOrSubquery,
  otherJoins: Seq[(SqliteJoinOperator, SqliteTableOrSubquery, SqliteJoinConstraint)]
) extends SqliteSelectComponent

case class SqliteJoinConstraint(
  joinExpression: Option[SqliteExpr] = None,
  byColumnNames: List[String] = List()
) extends SqliteSelectComponent

sealed trait SqliteJoinOperator
case class SqliteJoinLeft() extends SqliteJoinOperator
case class SqliteJoinRight() extends SqliteJoinOperator
case class SqliteJoinFull() extends SqliteJoinOperator
case class SqliteJoinInner(usingCommas: Boolean = false) extends SqliteJoinOperator
case class SqliteJoinCross() extends SqliteJoinOperator

case class SqliteWhereExpr(
  condition: SqliteExpr
) extends SqliteSelectComponent

case class SqliteGroupByExpr(
  groupingExprs: Seq[SqliteExpr]
) extends SqliteSelectComponent

case class SqliteHavingExpr(
  condition: SqliteExpr
) extends SqliteSelectComponent

case class SqliteOrderByExpr(
  orderingTerms: Seq[SqliteOrderingTerm]
) extends SqliteSelectComponent

case class SqliteOrderingTerm(
  expr: SqliteExpr,
  collationName: Option[String] = None,
  ascending: Boolean = true,
  nullsLast: Boolean = true,
) extends SqliteSelectComponent

case class SqliteLimitExpr(
  limitExpr: SqliteExpr,
  offsetExpr: Option[SqliteExpr] = None,
) extends SqliteSelectComponent

case class SqliteSetExpr(
  setOp: SqliteSetOperator,
  anotherSelect: SqliteSelectCore
) extends SqliteSelectComponent

sealed trait SqliteSetOperator
case class SqliteUnion() extends SqliteSetOperator
case class SqliteUnionAll() extends SqliteSetOperator
case class SqliteIntersect() extends SqliteSetOperator
case class SqliteExcept() extends SqliteSetOperator

// EXPR
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
  args: List[SqliteExpr] = List(),
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
case object AND extends SqliteBinaryOpSign
case object OR extends SqliteBinaryOpSign
case object UNKNOWN_BINOP extends SqliteBinaryOpSign

