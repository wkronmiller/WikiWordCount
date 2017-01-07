package net.kronmiller.william.wikiwordcount

import scala.io.Source
import java.io.File
import scala.collection.parallel.immutable.ParSeq

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.io.FileOutputStream
import java.io.PrintWriter

trait Counter {
  protected val NUM_SLICES = 30
  protected val wordRegex = "([a-zA-Z\\']+)".r
  protected val splitRegex = "\\s+"
}

object SparkCore extends Counter {
  def main(args: Array[String]) {
    val Array(xmlPath) = args
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wikiwordcount")
    val sc = new SparkContext(sparkConf)

    sc.textFile(xmlPath, NUM_SLICES)
      .flatMap(_.split(splitRegex))
      .map(_.trim)
      .map(wordRegex.findFirstIn(_))
      .filter(_.isDefined).map(_.get)
      .map(_.replace("''", ""))
      .filter(_.length > 0)
      .map((_, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile("counts")

    sc.stop
  }
}

class ConcurrentCounter[T] {
  private val countMap = scala.collection.mutable.Map[T, Int]()
  def increment(key: T) {
    countMap.synchronized {
      countMap(key) = countMap.getOrElse(key, 0) + 1
    }
  }
  def saveToFile(file: File) {
    val outStream = new PrintWriter(file)
    println("Saving to file")
    countMap.foreach { case (k, v) => outStream.write(s"$k -> $v") }
    println("Done saving to file")
    outStream.close
  }
}

class ConcurrentIterator[T](val iterator: Iterator[T]) {
  private def getOpt: Option[T] = {
    if (iterator.hasNext) {
      Some(iterator.next)
    } else {
      None
    }
  }
  def isEmpty: Boolean = iterator.synchronized(iterator.isEmpty)
  def get(n: Int): List[T] = {
    iterator
      .synchronized {
        (0 to n).map(_ => getOpt).toList
      }
      .filter(_.isDefined)
      .map(_.get)
  }
}

class CountExecutor(safeIterator: ConcurrentIterator[String], counter: ConcurrentCounter[String]) extends Runnable with Counter {
  val LINES_PER_ITERATION = 500
  private def processLine(line: String) = {
    line
      .split(splitRegex)
      .map(wordRegex.findFirstIn(_))
      .filter(_.isDefined).map(_.get)
      .map(_.replace("''", ""))
      .filter(_.length > 0)
      .foreach(counter.increment)
  }
  def run {
    println("Starting worker")
    while (safeIterator.isEmpty == false) {
      safeIterator
        .get(LINES_PER_ITERATION)
        .foreach(processLine)
    }
    println("Worker finished")
  }
}

object VanillaCore extends Counter {
  private val counter = new ConcurrentCounter[String]
  private val pool = Executors.newCachedThreadPool

  def main(args: Array[String]) {
    val Array(xmlPath) = args
    val unsafeIterator = Source
      .fromFile(xmlPath)
      .getLines
    val safeIterator = new ConcurrentIterator(unsafeIterator)

    val executors = (0 to NUM_SLICES)
      .map(_ => new CountExecutor(safeIterator, counter))

    println("Starting workers")
    executors.foreach(pool.execute)

    println("Waiting for completion")
    pool.shutdown
    pool.awaitTermination(10, TimeUnit.HOURS)
    println("All mappers finished")
    
    counter.saveToFile(new File("counts.txt")
  }
}