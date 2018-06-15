package com.ibm.cio

import org.json4s.jackson.JsonMethods.parse

object JumbleSolver {

  lazy val wordFrequencyJson = scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("freq_dict.json")).mkString
  lazy val wordFrequencyMap  = parse(wordFrequencyJson).values.asInstanceOf[Map[String, BigInt]]

  def main(args: Array[String]): Unit = {
    wordFrequencyMap.foreach(a => println(a))
  }

}
