package net.kronmiller.william.wikiwordcount

import scala.io.Source
import java.io.File
import scala.collection.parallel.immutable.ParSeq

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Parser {
  private val textRegex = "\\<text xml\\:space=\"preserve\"\\>(.*)\\</text\\>".r
  private val wordRegex = "[a-zA-Z\\']+"
  def getWords(source: String) = {
    textRegex
      .findAllIn(source)
      .toList
      .par
      .map(text => {
        val textRegex(isolatedText) = text
        isolatedText
      })
      .flatMap(_.split("\\s+").toList)
      .filter(_.matches(wordRegex))
      .map(_.replace("''", ""))
      .filter(_.length > 0)
  }
}

class XMLLoader(xmlPath: String) {
  def load = {
    Source
      .fromFile(new File(xmlPath))
      .getLines
      .mkString(" ")
  }
}

object Core {
  val NUM_SLICES = 30
  def main(args: Array[String]) {
    val Array(xmlPath) = args
    println("Loading text")
    val loader = new XMLLoader(xmlPath)
    println("Flatmapping words")
    val parWords: List[String] = Parser.getWords(loader.load).toList
    println("Starting spark")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wikiwordcount")
    val sc = new SparkContext(sparkConf)

    sc.parallelize[String](parWords, NUM_SLICES).map((_, 1)).reduceByKey(_ + _).saveAsTextFile("counts")

    sc.stop
  }
}