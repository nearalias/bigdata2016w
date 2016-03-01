package ca.uwaterloo.cs.bigdata2016w.nearalias.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

class ConfQ1(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "l_shipdate predicate", required = true)
}

object Q1 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfQ1(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)

    val date = args.date()
    val lineitem = sc.textFile(args.input()+"/lineitem.tbl")
    val result = lineitem
      .map(line => {
        if (line.split("""\|""")(10).startsWith(date)) ("*", 1) else ("*", 0)
      })
      .reduceByKey(_ + _)
      .first()
    println("Answer="+result._2);
  }
}
