import org.scalatest.flatspec._
import org.scalatest.matchers._
import fastparse._, NoWhitespace._
import dev.dskrzypiec.parser.Common._

abstract class UnitSpec extends AnyFlatSpec with should.Matchers

class CommonTests extends UnitSpec {
  // ws
  "single space" should "be treated as whitespace" in {
    assertResult(Parsed.Success((), 1)) {parse(" ", ws(_))}
  }
  "many spaces" should "be treated as whitespace" in {
    assertResult(Parsed.Success((), 3)) {parse("   ", ws(_))}
  }
  "single tab" should "be treated as whitespace" in {
    assertResult(Parsed.Success((), 1)) {parse("\t", ws(_))}
  }
  "many tabs" should "be treated as whitespace" in {
    assertResult(Parsed.Success((), 3)) {parse("\t\t\t", ws(_))}
  }
  "mix of spaces, tabs and new lines" should "be treated as whitespace" in {
    assertResult(Parsed.Success((), 4)) {parse(" \n\t ", ws(_))}
  }

  // digit
  "0" should "be a digit" in {
    assertResult(Parsed.Success((), 1)) {parse("0", digit(_))}
  }
  "9" should "be a digit" in {
    assertResult(Parsed.Success((), 1)) {parse("9", digit(_))}
  }
  "a" should "not be a digit" in {
    parse("a", digit(_)) shouldBe a [Parsed.Failure]
  }

  //hex prefix
  "0x" should "be a hex prefix" in {
    assertResult(Parsed.Success((), 2)) {parse("0x", hexPrefix(_))}
  }
  "0X" should "be a hex prefix" in {
    assertResult(Parsed.Success((), 2)) {parse("0X", hexPrefix(_))}
  }

  // hex number parser
  "digit" should "be a hex char" in {
    assertResult(Parsed.Success((), 1)) {parse("8", hexChar(_))}
  }
  "lowercase a-f" should "be a hex char" in {
    assertResult(Parsed.Success((), 1)) {parse("d", hexChar(_))}
  }
  "uppercase A-F" should "be a hex char" in {
    assertResult(parse("F", hexChar(_))) {Parsed.Success((), 1)}
  }
  "Letters > F" should "not be a hex char" in {
    parse("Z", hexChar(_)) shouldBe a [Parsed.Failure]
  }

  // dot parser
  "A single dot string" should "be properly parsed" in {
    assertResult(Parsed.Success((), 1)) {parse(".", dot(_))}
  }
  "A two dot string" should "parse only first dot" in {
    assertResult(Parsed.Success((), 1)) {parse("..", dot(_))}
  }
  "A single comma string" should "not be Parsed.Success" in {
    assert(parse(",", dot(_)) != Parsed.Success((), 1))
  }
}
