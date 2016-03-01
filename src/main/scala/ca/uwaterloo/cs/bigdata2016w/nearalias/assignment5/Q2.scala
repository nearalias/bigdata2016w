package ca.uwaterloo.cs.bigdata2016w.nearalias.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

class ConfQ2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "l_shipdate predicate", required = true)
}

object Q2 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfQ2(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q2")
    val sc = new SparkContext(conf)

    val date = args.date()

    val lineitem = sc.textFile(args.input()+"/lineitem.tbl")
    val lineitemResults = lineitem
      .filter(line => {
        line.split("""\|""")(10).startsWith(date)
      }).
      map(line => (line.split("""\|""")(0), 1))
    
    val orders = sc.textFile(args.input()+"/orders.tbl")
    val ordersResults = orders
      .map(line => {
        val params = line.split("""\|""")
        (params(0), params(6))
      })
      .cogroup(lineitemResults)
      .filter(line => { !(line._2._2.isEmpty) })
      .map(line => (line._1.toInt, line._2._1.mkString))
      .sortByKey(true)
      .map(line => (line._2, line._1))
      .take(20)

    for (x <- ordersResults) {
      println(x);
    }
  }
}
