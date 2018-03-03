package io.github.dataramblers

import com.typesafe.config.Config

import scala.util.{Success, Try}
import language.postfixOps

object BoostFuzzinessIterator {

  def initialize(config: Config): List[(String, Double, String, Double, Double, Double, Double)] = {
    for {
      titleFuzziness <- getFuzzinessSteps(
        Try(config.getInt("reconciling.title.fuzziness.from")),
        Try(config.getInt("reconciling.title.fuzziness.to")),
        Try(config.getInt("reconciling.title.fuzziness.step"))
      )
      titleBoost <- getBoostSteps(
        Try(config.getDouble("reconciling.title.boost.from")),
        Try(config.getDouble("reconciling.title.boost.to")),
        Try(config.getDouble("reconciling.title.boost.step"))
      )
      personFuzziness <- getFuzzinessSteps(
        Try(config.getInt("reconciling.person.fuzziness.from")),
        Try(config.getInt("reconciling.person.fuzziness.to")),
        Try(config.getInt("reconciling.person.fuzziness.step"))
      )
      personBoost <- getBoostSteps(
        Try(config.getDouble("reconciling.person.boost.from")),
        Try(config.getDouble("reconciling.person.boost.to")),
        Try(config.getDouble("reconciling.person.boost.step"))
      )
      isbnBoost <- getBoostSteps(
        Try(config.getDouble("reconciling.isbn.boost.from")),
        Try(config.getDouble("reconciling.isbn.boost.to")),
        Try(config.getDouble("reconciling.isbn.boost.step"))
      )
      issnBoost <- getBoostSteps(
        Try(config.getDouble("reconciling.issn.boost.from")),
        Try(config.getDouble("reconciling.issn.boost.to")),
        Try(config.getDouble("reconciling.issn.boost.step"))
      )
      dateBoost <- getBoostSteps(
        Try(config.getDouble("reconciling.date.boost.from")),
        Try(config.getDouble("reconciling.date.boost.to")),
        Try(config.getDouble("reconciling.date.boost.step"))
      )
    } yield (
      titleFuzziness,
      titleBoost,
      personFuzziness,
      personBoost,
      isbnBoost,
      issnBoost,
      dateBoost
    )
  }

  private def getBoostSteps(start: Try[Double], end: Try[Double], step: Try[Double]): List[Double] = (start, end, step) match {
    case (Success(x), Success(y), Success(z)) => x to y by z toList
    case _ => List(1)
  }

  private def getFuzzinessSteps(start: Try[Int], end: Try[Int], step: Try[Int]): List[String] = (start, end, step) match {
    case (Success(x), Success(y), Success(z)) => (x to y by y toList) map (x => x.toString)
    case _ => List("0")
  }

}
