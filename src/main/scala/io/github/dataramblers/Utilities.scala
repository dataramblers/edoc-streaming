package io.github.dataramblers

object Utilities {

  /**
    * Converts a ISBN10 string to ISBN13
    * If a ISBN10 is not complete simply return the value.
    * @param isbn10 ISBN10
    * @return ISBN13
    */
  def isbn10ToIsbn13(isbn10: String): String = {
    val isbn10_reduced = isbn10.replaceAll("-","")
    if (isbn10_reduced.length == 10){
      val isbn13 = "978" + isbn10_reduced.substring(0, 9)
      val sum = (for (i <- Range(0, isbn13.length))
        yield (isbn13.charAt(i).toInt - 48) * (if (i % 2 == 0) 1 else 3)).sum
      isbn13 + (10 - (sum % 10))
    }
    else
      isbn10
  }

}
