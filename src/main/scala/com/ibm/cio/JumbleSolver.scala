/*
*
* Bremer Jonathan
*
*/
package com.ibm.cio

import com.ibm.cio.WordDictionary._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

object JumbleSolver {

  val LEAST_FREQUENCY_SCORE  = 9887
  val createJumbleTuplesUDF  = udf(createJumbleTuples _)
  val unscrambleJumblesUDF   = udf(unscrambleJumbles _)
  val createAnswerJumbleUDF  = udf(createAnswerJumble _)
  val findPossibleAnswersUDF = udf(findPossibleAnswers _)
  val findProbableAnswerUDF  = udf(findProbableAnswer _)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("jumblesolver")//.set("spark.network.timeout", "600s").setMaster("local[*]")
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
    val probableAnswer     = possibleAnswers.withColumn("probable_answer", findProbableAnswerUDF(possibleAnswers("possible_answers"))).select("jumbles", "unscrambled_jumbles", "answer_jumbles", "probable_answer")
    probableAnswer.write.option("header","true").csv(args(1))
    //probableAnswer.show(false)
  }

  // Converts a string of jumbles in the format "abc(1,2),de(3,4)" to an array of tuples [(abc,[1 ,2, 3]), (de,[3,4])]
  def createJumbleTuples(jumbles: String): Seq[(String, Seq[Int])] = {
    jumbles.split("\\)\\s*,\\s*")
      .map(x => (x.replaceAll("\\((.)*", ""), x.replaceAll("([^\\(])*\\(|(\\D)*\\)", "")))
      .map(x => (x._1, x._2.split("\\s*,\\s*").map(_.toInt).toSeq))
  }

  def unscrambleJumbles(jumbles: Seq[Row]): Seq[((Seq[String], Int), Seq[Int])] = {
    jumbles.map(x => (unscrambleJumble(x.getAs[String](0)),  x.getAs[Seq[Int]](1)))
  }

  def createAnswerJumble(unscrambledJumbleRows: Seq[Row]): Seq[String] = {
    var jumble: String = ""
    val unscrambledJumbles: Seq[((Seq[String], Int), Seq[Int])] = unscrambledJumbleRows.map(r1 => (  (r1.getAs[Row](0).getAs[Seq[String]](0).map(r2 => charsAt(r2, r1.getAs[Seq[Int]](1))), r1.getAs[Row](0).getInt(1)) ,   r1.getAs[Seq[Int]](1)  ))
    var answerJumble: Seq[String] = unscrambledJumbles(0)._1._1//.to[ListBuffer]
    for (i <- 1 until unscrambledJumbles.length) {
      answerJumble = answerJumble cross unscrambledJumbles(i)._1._1//.to[ListBuffer]
    }
    answerJumble.distinct
  }

  def findAnswerWordsJumbles(answerJumblePart: String, answerWordsLengths: Array[Int], answerWordIndex: Int): Seq[String] = {
    val wordKeys = selectWordKeysOfLengthAndCombosOfSortByFrequency(answerWordsLengths(answerWordIndex), answerJumblePart)
    if (answerWordIndex == (answerWordsLengths.length - 1)) {
      return if (wordKeys.nonEmpty) Seq(wordKeys(0)) else Seq.empty[String]
    }
    else {
      for (wordKey <- wordKeys) {
        val answerWordJumble = findAnswerWordsJumbles(removeChars(answerJumblePart, wordKey), answerWordsLengths, answerWordIndex + 1)
        if (answerWordJumble.nonEmpty) {
          return wordKey +: answerWordJumble
        }
      }
      return Seq.empty[String]
    }
  }

  def findPossibleAnswers(answerWordsLengths: String, answerJumbles: Seq[String]): Seq[(String, Int)] = {
    val answerLengths: Array[Int] = answerWordsLengths.split("\\s*,\\s*").map(_.toInt)
    val numAnswers = answerLengths.length // number of answers per jumble
    // Making word combos with any zero frequency score (unscored) words have more total frequency than same number scored word combos.
    val zeroFrequencyScoreOffset = LEAST_FREQUENCY_SCORE * answerLengths.length + 1
    var answers = ListBuffer.empty[(String, Int)]
    for (answerJumble <- answerJumbles) {
      var answer = ""
      var freq = 0
      findAnswerWordsJumbles(answerJumble, answerLengths, 0)
        .map(unscrambleJumble(_))
        .map(x => { (x._1.mkString("/"), if (x._2 == 0) zeroFrequencyScoreOffset else x._2) })
        .foreach(x => { answer = answer + " " + x._1; freq += x._2 })
      answers += (("'" + answer.trim + "'", freq))
    }
    answers
  }

  private def removeChars(string: String, regex: String): String = {
    var result = string
    regex.foreach(char => result = result.replaceFirst(char.toString, ""))
    result
  }

  // Finds possible answer word combos with the least total frequency
  def findProbableAnswer(possibleAnswers: Seq[Row]): String = {
    var answer: (String, Int) = null
    possibleAnswers
      .map(r => (r.getString(0), r.getInt(1)))
      .foreach(x => {
        if (answer == null || answer._2 > x._2) {
          answer = x
        }
        else {
          if (answer._2 == x._2) {
            answer = (answer._1 + " OR " + x._1, x._2)
          }
        }
      })
    if (answer != null) answer._1 else null
  }

  implicit class Crossable[X](x1: Seq[String]) {
    def cross[X](x2: Seq[String]) = {for (x <- x1; y <- x2) yield (x + y)}
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
