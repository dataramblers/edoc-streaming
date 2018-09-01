package io.github.dataramblers

import scala.util.{Failure, Success, Try}

object Utilities {

  /**
    * Converts a ISBN10 string to ISBN13
    * If a ISBN10 is not complete simply return the value.
    *
    * @param isbn10 ISBN10
    * @return ISBN13
    */
  def isbn10ToIsbn13(isbn10: String): String = {
    val isbn10_reduced = isbn10.replaceAll("-", "")
    if (isbn10_reduced.length == 10) {
      val isbn13 = "978" + isbn10_reduced.substring(0, 9)
      val sum = (for (i <- Range(0, isbn13.length))
        yield (isbn13.charAt(i).toInt - 48) * (if (i % 2 == 0) 1 else 3)).sum
      isbn13 + (10 - (sum % 10))
    }
    else
      isbn10
  }

  /**
    * Checks whether ISSN is syntactically valid
    *
    * @param issn ISSN
    * @return True if ok
    */
  def isValidISSN(issn: String): Boolean = {
    val regex = "^\\d{4}-\\d{3}[\\dxX]$".r
    regex.findFirstIn(issn) match {
      case Some(t) =>
        val asInt = Try(t.charAt(9).toInt)

        def check(result: Try[Int]): Int = {
          asInt match {
            case Success(x) => x
            case Failure(_) => 10
          }
        }

        val total = t(0).toInt * 8 + t(1).toInt * 7 + t(2).toInt * 6 + t(3).toInt * 5 + t(5) * 4 + t(6) * 3 + t(7) * 2
        val checkDigit = 11 - (total % 11)
        if (checkDigit == check(asInt)) true
        else false
      case None =>
        false
    }
  }
}
