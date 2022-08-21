package dev.dskrzypiec.parser.sqlite

import fastparse._, NoWhitespace._
import dev.dskrzypiec.parser.Common._
import dev.dskrzypiec.parser.sqlite.Keyword._

object Identifier {
  def id[_ : P]: P[String] = {
    P( !keyword ~
      (lowAz | upAz | underscore).rep(1) ~
      (lowAz | upAz | underscore | digit | dollar).rep).!
  }
}
