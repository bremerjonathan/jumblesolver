package com.ibm.cio

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}
import scala.util.parsing.json.JSON

object JumbleSolver {

  lazy val wordFrequencyJson = scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("freq_dict.json")).mkString
  lazy val wordFrequencyMap  = JSON.parseFull(wordFrequencyJson).get.asInstanceOf[Map[String, Double]]

  val LEAST_FREQUENCY_SCORE  = 9887
  val createJumbleTuplesUDF  = udf(createJumbleTuples _)
  val unscrambleJumblesUDF   = udf(unscrambleJumbles _)
  val createAnswerJumbleUDF  = udf(createAnswerJumble _)
  val findPossibleAnswersUDF = udf(findPossibleAnswers _)
  val findProbableAnswerUDF  = udf(findProbableAnswer _)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("jumblesolver")//.setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val puzzle = sparkSession.read
      .option("ignoreLeadingWhiteSpace","true")
      .option("ignoreTrailingWhiteSpace","true")
      .option("escape", "\"")
      .option("header", "true")
      //.csv("input/*.csv")
    .csv(args(0))

    val jumbleTuples       = puzzle.withColumn("jumble_tuples", createJumbleTuplesUDF(puzzle("jumbles")))
    val unscrambledJumbles = jumbleTuples.withColumn("unscrambled_jumbles", unscrambleJumblesUDF(jumbleTuples("jumble_tuples"))).drop("jumble_tuples")
    val answerJumbles      = unscrambledJumbles.withColumn("answer_jumbles", createAnswerJumbleUDF(unscrambledJumbles("unscrambled_jumbles")))
    val possibleAnswers    = answerJumbles.withColumn("possible_answers", findPossibleAnswersUDF(answerJumbles("answer_words_sizes"), answerJumbles("answer_jumbles")))
    val probableAnswer     = possibleAnswers.withColumn("probable_answer", findProbableAnswerUDF(possibleAnswers("possible_answers")))
    probableAnswer.rdd.saveAsTextFile("output")

  }

  // Converts a string of jumbles in the format "abc(1,2),de(3,4)" to an array of tuples [(abc,[1 ,2, 3]), (de,[3,4])]
  def createJumbleTuples(jumbles: String): Seq[(String, Seq[Int])] = {
    jumbles.split("\\)\\s*,\\s*")
      .map(x => (x.replaceAll("\\((.)*", ""), x.replaceAll("([^\\(])*\\(|(\\D)*\\)", "")))
      .map(x => (x._1, x._2.split("\\s*,\\s*").map(_.toInt).toSeq))
  }

  def unscrambleJumbles(jumbles: Seq[Row]): Seq[(Seq[(String, Double)], Seq[Int])] = {
    jumbles.map(x => {
      (unscrambleJumble(x.getAs[String](0)), x.getAs[Seq[Int]](1))
    })
  }

  // This method the most frequent permutation word from the word frequency map (i.e. wordFrequencyMap)
  def unscrambleJumble(jumble: String): Seq[(String, Double)] = {
    val unscrambledJumbles: ListBuffer[(String, Double)] = ListBuffer()
    for (permutation: String <- jumble.toLowerCase.permutations if wordFrequencyMap.contains(permutation)) {
      if (unscrambledJumbles.isEmpty || wordFrequencyMap(permutation) == unscrambledJumbles(0)._2) {
        unscrambledJumbles += ((permutation, wordFrequencyMap(permutation)))
      }
      else if (unscrambledJumbles(0)._2 == 0 || (wordFrequencyMap(permutation) != 0 && wordFrequencyMap(permutation) < unscrambledJumbles(0)._2)) {
        unscrambledJumbles.clear()
        unscrambledJumbles += ((permutation, wordFrequencyMap(permutation)))
      }
    }
    unscrambledJumbles
  }

  def createAnswerJumble(unscrambledJumbleRows: Seq[Row]): Seq[String] = {
    var jumble: String = ""
    var answerJumble: ListBuffer[(String, Double)] = ListBuffer() //unscrambledJumbles(0).getAs[Seq[(String, Double)]]("_1")
    val unscrambledJumbles: Seq[(Seq[(String, Double)], Seq[Int])] = unscrambledJumbleRows.map(r1 => ((r1.getAs[Seq[Row]](0).map(r2 => ((charsAt(r2.getString(0), r1.getAs[Seq[Int]](1)), r2.getDouble(1)))), r1.getAs[Seq[Int]](1))))
    answerJumble = unscrambledJumbles(0)._1.to[ListBuffer]
    for (i <- 1 until unscrambledJumbles.length) {
      answerJumble = answerJumble cross unscrambledJumbles(i)._1.to[ListBuffer]
    }
    answerJumble.map(_._1).distinct
  }

  def findPossibleAnswers(answerWordsSizesString: String, answerJumbles: Seq[String]): Array[(String, Double)] = {
    val answerWordsSizes: Array[Int] = answerWordsSizesString.split("\\s*,\\s*").map(_.toInt)
    val answerWords = new Array[String](answerWordsSizes.length)
    val adjustedZeroFrequencyScore = LEAST_FREQUENCY_SCORE * answerWordsSizes.length + 1
    var answerWordsWithMinTotalScore = new Array[ListBuffer[(String, Double)]](answerJumbles.length)
    for ((answerJumble, index) <- answerJumbles.zipWithIndex) {
      answerWordsWithMinTotalScore(index) = ListBuffer[(String, Double)]()
      //TODO: Find better algorithm that avoids iterating over potentially huge number of permutations
      for (permutation <- answerJumble.toLowerCase.permutations) {
        var permutationSlice = permutation
        for ((answerWordSize, index) <- answerWordsSizes.zipWithIndex) {
          answerWords(index) = permutationSlice.slice(0, answerWordSize)
          permutationSlice = permutationSlice.slice(answerWordSize, permutationSlice.length)
        }
        if (contains(wordFrequencyMap, answerWords)) {
          var answerWordsAndTotalScore = answerWords.map(x => (x, wordFrequencyMap.get(x).get))
            .map(x => (x._1, if (x._2 == 0) (x._2 + adjustedZeroFrequencyScore) else x._2))
            .reduce[(String, Double)]((x1, x2) => (x1._1 + " " + x2._1, x1._2 + x2._2))
          if (answerWordsWithMinTotalScore(index).isEmpty || answerWordsAndTotalScore._2 == answerWordsWithMinTotalScore(index).head._2) {
            answerWordsWithMinTotalScore(index) += answerWordsAndTotalScore
          }
          else if (answerWordsAndTotalScore._2 < answerWordsWithMinTotalScore(index).head._2) {
            answerWordsWithMinTotalScore(index).clear()
            answerWordsWithMinTotalScore(index) += answerWordsAndTotalScore
          }
        }
      }
    }
    answerWordsWithMinTotalScore.flatten
  }

  def findProbableAnswer(possibleAnswers: Seq[Row]): String = {
    possibleAnswers
      .map(r => (r.getString(0), r.getDouble(1)))
      .reduce[(String, Double)]((x1, x2) => if (x1._2 <= x2._2) x1 else x2)
      ._1
  }

  implicit class Crossable[X](x1: ListBuffer[(String, Double)]) {
    def cross[X](x2: ListBuffer[(String, Double)]) = for { x <- x1; y <- x2 } yield (x._1 + y._1, x._2 + y._2)
  }

  private def charsAt(string: String, indices: Seq[Int]): String = {
    var tmp = ""
    indices.foreach(i => {
      tmp += string.charAt(i).toString
    })
    tmp
  }

  private def contains(map: Map[String, Any], keys: Array[String]): Boolean = {
    var hasKey = true
    breakable {
      for (key <- keys) {
        if (!map.contains(key)) {
          hasKey = false
          break
        }
      }
    }
    hasKey
  }

}
