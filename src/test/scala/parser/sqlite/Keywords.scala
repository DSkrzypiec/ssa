import org.scalatest.flatspec._
import org.scalatest.matchers._
import fastparse._, NoWhitespace._
import dev.dskrzypiec.parser.sqlite._
import dev.dskrzypiec.parser.sqlite.Keyword.keyword

class KeywordsTests extends UnitSpec {
  "select" should "be a keyword" in {
    assertResult(Parsed.Success("select", 6)) { parse("select", keyword(_)) }
  }
  "select_" should "not be a keyword" in {
    parse("select_", keyword(_)) shouldBe a [Parsed.Failure]
  }
}
