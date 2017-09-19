package io.github.dataramblers

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._


object EdocStreaming {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val edocRecords = env.readTextFile("/home/schuepbs/temp/hackathon17/all.small.json.gz")

    val test = edocRecords
      .map(x => JsonParser.toEdoc(x))
      .filter(x => x.doi.isEmpty)
      .map(x => ESLookup.lookup(x, "crossref", "crossref"))

    // FIXME: Just for testing
    test.print()
  }

}
