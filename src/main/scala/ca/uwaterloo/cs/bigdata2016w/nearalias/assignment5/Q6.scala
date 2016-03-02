package ca.uwaterloo.cs.bigdata2016w.nearalias.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

class ConfQ6(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "l_shipdate predicate", required = true)
}

object Q6 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfQ6(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q6")
    val sc = new SparkContext(conf)

    val date = args.date()

    // returnflag + linestatus -> (quantity, extendedprice, discount, tax)
    val lineitem = sc.textFile(args.input()+"/lineitem.tbl")
    val lineitemData = lineitem
      .filter(line => { line.split("""\|""")(10).startsWith(date) })
      .map(line => {
        val params = line.split("""\|""")
        (params(8)+":"+params(9), (params(4), params(5), params(6), params(7)))
      })

    val result = lineitemData
      .map(line => {
        val discount = line._2._3.toDouble
        val tax = line._2._4.toDouble

        val qty = line._2._1.toDouble
        val base_price = line._2._2.toDouble
        val disc_price = base_price * (1 - discount)
        val charge = base_price * (1 - discount) * (1 + tax)
        (line._1, (1.0, qty, base_price, disc_price, charge, discount))
      })
      .reduceByKey((a, b) => {
        (a._1+b._1, a._2+b._2, a._3+b._3, a._4+b._4, a._5+b._5, a._6+b._6)
      })
      .map(line => {
        val returnflag = line._1.split(":")(0)
        val linestatus = line._1.split(":")(1)
        val sum_qty = line._2._2
        val sum_base_price = line._2._3
        val sum_disc_price = line._2._4
        val sum_charge = line._2._5
        val count_order = line._2._1
        val avg_qty = sum_qty / count_order
        val avg_price = sum_base_price / count_order
        val avg_disc = line._2._6 / count_order
        (returnflag, linestatus, sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count_order)
      })
      .collect()

    for (x <- result) {
      println(x)
    }
  }
}
