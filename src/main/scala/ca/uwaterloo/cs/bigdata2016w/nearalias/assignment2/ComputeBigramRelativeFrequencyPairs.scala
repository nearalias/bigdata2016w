package ca.uwaterloo.cs.bigdata2016w.nearalias.assignment2

import ca.uwaterloo.cs.bigdata2016w.nearalias.assignment2.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
}

class MyPartitioner(numReducers: Int) extends Partitioner {
  def numPartitions = numReducers
  def getPartition(key: Any): Int = {
    key match {
      case (firstWord, secondWord) => (firstWord.hashCode() & Int.MaxValue) % numPartitions
    }
  }
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Compute Bigram Relative Frequency Pairs")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        val wordStar = if (tokens.length > 1) tokens.take(tokens.length-1).map(word => (word, "*")).toList else List()
        val bigrams = if (tokens.length > 1) tokens.sliding(2).map(pair => (pair.head, pair.tail.head)).toList else List()
        wordStar ++ bigrams
      })
      .map(bigram => (bigram, 1))
      .reduceByKey(_ + _)
      .sortBy(_._1)
      .partitionBy(new MyPartitioner(args.reducers()))
      .mapPartitions( iter => {
        var result = collection.mutable.MutableList[((String, String), Float)]()
        var marginalMap = collection.mutable.Map[String, Int]()
        while (iter.hasNext) {
          val bigram = iter.next
          if (bigram._1._2 == "*") {
            marginalMap(bigram._1._1) = bigram._2
            val newBigram = (bigram._1, bigram._2.toFloat)
            result += newBigram
          } else {
            val newBigram = (bigram._1, bigram._2.toFloat / marginalMap(bigram._1._1))
            result += newBigram
          }
        }
        result.iterator
      })

    counts.saveAsTextFile(args.output())
  }
}
