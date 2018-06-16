package com.ibm.cio

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.JsonMethods.parse

object JumbleSolver {

  lazy val wordFrequencyJson = scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("freq_dict.json")).mkString
  lazy val wordFrequencyMap  = parse(wordFrequencyJson).values.asInstanceOf[Map[String, BigInt]]

  def main(args: Array[String]): Unit = {
    //TODO: unset master from local
    val conf = new SparkConf().setAppName("jumblesolver").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val puzzle = sparkSession.read
      .option("ignoreLeadingWhiteSpace","true")
      .option("ignoreTrailingWhiteSpace","true")
      .option("escape", "\"")
      .option("header", "true")
      //TODO: unhardcord input path. Get it as the 1st command line argument.
      .csv("input/*.csv")

    puzzle.printSchema()
    puzzle.show(false)

  }

}
