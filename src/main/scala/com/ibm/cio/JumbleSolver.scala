package com.ibm.cio

import com.ibm.cio.JumbleSolver.{createJumbleTuplesUDF, unscrambleJumble, unscrambleJumblesUDF}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Row, SparkSession}
import org.json4s.jackson.JsonMethods.parse

import scala.util.control.Breaks.{break, breakable}

object JumbleSolver {

  lazy val wordFrequencyJson = scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("freq_dict.json")).mkString
  lazy val wordFrequencyMap  = parse(wordFrequencyJson).values.asInstanceOf[Map[String, BigInt]]

  val createJumbleTuplesUDF = udf(createJumbleTuples _)
  val unscrambleJumblesUDF  = udf(unscrambleJumbles _)

  def main(args: Array[String]): Unit = {
    //TODO: unset master from local
    val conf = new SparkConf().setAppName("jumblesolver").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val puzzle = sparkSession.read
      .option("ignoreLeadingWhiteSpace","true")
      .option("ignoreTrailingWhiteSpace","true")
      .option("escape", "\"")
      .option("header", "true")
      //TODO: unhardcode input path. Get it as the 1st command line argument.
      .csv("input/*.csv")

    val jumbleTuples       = puzzle.withColumn("jumble_tuples", createJumbleTuplesUDF(puzzle("jumbles")))
    val unscrambledJumbles = jumbleTuples.withColumn("unscrambled_jumbles", unscrambleJumblesUDF(jumbleTuples("jumble_tuples"))).drop("jumble_tuples")

    unscrambledJumbles.show(false)

  }

  // Converts a string of jumbles in the format "abc(1,2),de(3,4)") to an array of tuples [(abc,[1 ,2, 3]), (de,[3,4])]
  def createJumbleTuples(jumbles: String): Seq[(String, Seq[Int])] = {
    jumbles.split("\\)\\s*,\\s*")
      .map( x => (x.replaceAll("\\((.)*", ""), x.replaceAll("([^\\(])*\\(|(\\D)*\\)", "")) )
      .map(y => (y._1, y._2.split("\\s*,\\s*").map(_.toInt).toSeq))
  }

  def unscrambleJumbles(jumbles: Seq[Row]): Seq[(String, Seq[Int])] = {
    jumbles.map(x => {
      (unscrambleJumble(x.getAs[String]("_1"))._1, x.getAs[Seq[Int]]("_2"))
    })
  }

  // This method the most frequent permutation word from the word frequency map (i.e. wordFrequencyMap)
  def unscrambleJumble(jumble: String): (String, BigInt) = {
    var word: String    = null
    var wordFrequency: BigInt = -1

    breakable {
      for (permutation: String <- jumble.toLowerCase.permutations if wordFrequencyMap.contains(permutation)) {
        if (wordFrequencyMap(permutation) == 1) {
          word = permutation
          break
        }
        if (wordFrequency < wordFrequencyMap(permutation)) {
          word = permutation
          wordFrequency = wordFrequencyMap(permutation)
        }
      }
    }
    (word, wordFrequency)
  }

}
