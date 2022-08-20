package dev.dskrzypiec.parser

import fastparse._, NoWhitespace._


object Common {
  def lowAz[_ : P] = P(CharIn("a-z"))
  def upAz[_ : P] = P(CharIn("A-Z"))
  def ws[_ : P] = P(" " | "\t" | "\n" | "\r").rep
  def digit[_ : P] = P(CharIn("0-9"))
  def hexPrefix[_ : P] = P("0" ~ IgnoreCase("x"))
  def hexChar[_ : P] = P(digit | CharIn("a-f") | CharIn("A-F"))
  def dot[_ : P] = P(".")
}
