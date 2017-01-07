package net.kronmiller.william.wikiwordcount

import scala.io.Source
import java.io.File
import scala.collection.parallel.immutable.ParSeq

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scala.util.Failure
import scala.util.Success

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
  private val textRegex = "\\<text xml\\:space=\"preserve\"\\>(.*)\\</text\\>".r
  private val wordRegex = "([a-zA-Z\\']+)".r
  def getWords(source: String) = {
    textRegex
      .findAllIn(source)
  }

  def main(args: Array[String]) {
    val Array(xmlPath) = args
    println("Loading text")
    val loader = new XMLLoader(xmlPath)
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wikiwordcount")
    val sc = new SparkContext(sparkConf)

    sc.textFile(xmlPath, NUM_SLICES)
      .flatMap(_.split("\\s+"))
      .map(_.trim)
      .map(wordRegex.findFirstIn(_))
      .filter(_.isDefined).map(_.get)
      .map(_.replace("''", ""))
      .filter(_.length > 0)
      .map((_, 1)).reduceByKey(_ + _)
      .saveAsTextFile("counts")

    sc.stop
  }
}