package dev.dskrzypiec.parser

import fastparse._, NoWhitespace._


object Common {
  def lowAz[_ : P] = P(CharIn("a-z"))
  def upAz[_ : P] = P(CharIn("A-Z"))
  def ws[_ : P] = P(" " | "\t" | "\n" | "\r").rep
  def peekWs[_ : P] = P(&(" " | "\t" | "\n" | "\r") | End)
  def digit[_ : P] = P(CharIn("0-9"))
  def hexPrefix[_ : P] = P("0" ~ IgnoreCase("x"))
  def hexChar[_ : P] = P(digit | CharIn("a-f") | CharIn("A-F"))
  def dot[_ : P] = P(".")
  def comma[_ : P] = P(",")
  def star[_ : P] = P("*")
  def underscore[_ : P] = P("_")
  def openParen[_ : P] = P("(")
  def closeParen[_ : P] = P(")")
  def dollar[_ : P] = P("$")
}
