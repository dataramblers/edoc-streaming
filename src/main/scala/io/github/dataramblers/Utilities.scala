package io.github.dataramblers

object Utilities {

  /**
    * Converts a ISBN10 string to ISBN13
    * @param isbn10 ISBN10
    * @return ISBN13
    */
  def isbn10ToIsbn13(isbn10: String): String = {
    val isbn13 = "978" + isbn10.replaceAll("-","").substring(0, 9)
    val sum = (for (i <- Range(0, isbn13.length))
      yield (isbn13.charAt(i).toInt - 48) * (if (i % 2 == 0) 1 else 3)).sum
    isbn13 + (10 - (sum % 10))
  }

}
