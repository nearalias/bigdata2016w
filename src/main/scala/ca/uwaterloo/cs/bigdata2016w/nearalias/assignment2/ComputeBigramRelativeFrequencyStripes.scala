package ca.uwaterloo.cs.bigdata2016w.nearalias.assignment2

import ca.uwaterloo.cs.bigdata2016w.nearalias.assignment2.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

class ConfStripes(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
}

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfStripes(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Compute Bigram Relative Frequency Stripes")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        val stripes = collection.mutable.Map[String, collection.mutable.Map[String, Float]]()
        if (tokens.length > 1) {
          tokens.sliding(2).foreach { bigram =>
            val firstWord = bigram(0)
            val secondWord = bigram(1)
            if (stripes.contains(firstWord)) {
              val stripe = stripes(firstWord)
              if (stripe.contains(secondWord)) {
                stripe(secondWord) += 1.0f
                if (secondWord == "misleader") {
                }
              } else {
                stripe(secondWord) = 1.0f
              }
            } else {
              val stripe = collection.mutable.Map[String, Float]()
              stripe(secondWord) = 1.0f
              stripes(firstWord) = stripe
            }
          }
        }
        val result = collection.mutable.MutableList[(String, collection.mutable.Map[String, Float])]()
        stripes.foreach { keyVal =>
          val pair = (keyVal._1, keyVal._2)
          result += pair
        }
        result
      })
      .reduceByKey{ (a, b) =>
        b.foreach { keyVal =>
          val key = keyVal._1
          val value = keyVal._2
          if (a.contains(key)) {
            a(key) += value
          } else {
            a(key) = value
          }
        }
        a
      }
      .map{ keyVal =>
        var sum = 0.0f
        val word = keyVal._1
        val stripe = keyVal._2
        stripe.foreach { bigrams =>
          sum += bigrams._2
        }
        val newStripe = collection.mutable.Map[String, Float]()
        stripe.foreach { bigrams =>
          val secondWord = bigrams._1
          val freq = bigrams._2 / sum
          newStripe(secondWord) = freq
        }
        collection.mutable.Map(word -> newStripe)
      }

    counts.saveAsTextFile(args.output())
  }
}
