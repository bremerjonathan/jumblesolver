package com.ibm.cio

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON

object WordDictionary {

  lazy val wordMap = createWordMap()

  private def createWordMap(): mutable.HashMap[Int, mutable.HashMap[String, (ListBuffer[String], Int)  ]] = {
    val map = mutable.HashMap.empty[Int, mutable.HashMap[String, (ListBuffer[String], Int)   ]]
    val wordsJson = scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("freq_dict.json")).mkString
    JSON.globalNumberParser = {input => input.toInt}
    val words  = JSON.parseFull(wordsJson).get.asInstanceOf[Map[String, Int]]
    for((word, freq) <- words) {
      val kSorted = word.trim.sorted
      if (!map.contains(word.length)) {
        map += (word.length -> mutable.HashMap.empty[String, (ListBuffer[String], Int) ])
      }
      if (!map.get(word.length).get.contains(kSorted)) {
        map.get(word.length).get += (kSorted -> (ListBuffer(word), freq) )
      }
      else {
        if (map.get(word.length).get.get(kSorted).get._2 == freq) {
          map.get(word.length).get.get(kSorted).get._1 += word
        }
        else {
          if ((map.get(word.length).get.get(kSorted).get._2 == 0 && freq != 0) || map.get(word.length).get.get(kSorted).get._2 > freq) {
            map.get(word.length).get += (kSorted -> (ListBuffer(word), freq))
          }
        }
      }
    }
    return map
  }

  def unscrambleJumble(jumble: String): (Seq[String], Int) = {
    wordMap.get(jumble.length).get.get(jumble.toLowerCase.sorted).get
  }

  def selectWordKeysOfLengthAndCombosOfSortByFrequency(wordKeyLength: Int, letters: String): Seq[String] = {
    wordMap.get(wordKeyLength).get
      .filterKeys(containChars(letters, _))
      .map(x => (x._1, x._2._2))
      //.filter(x => (x._2 != 0 && x._2 < 775))
      //.filter(x => (x._2 != 0))
      .toSeq
      .sortWith((x1, x2) => x2._2 == 0 || (x1._2 != 0 && x1._2 < x2._2))
      .map(_._1)
  }

  private def containChars(string: String, chars: String): Boolean = {
    var str1 = string
    for (char: Char <- chars) {
      if (!str1.contains(char)) return false
      str1 = str1.replaceFirst(char.toString,"")
    }
    true
  }

}
