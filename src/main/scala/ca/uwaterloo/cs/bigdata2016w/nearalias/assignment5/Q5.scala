package ca.uwaterloo.cs.bigdata2016w.nearalias.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

class ConfQ5(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input)
  val input = opt[String](descr = "input path", required = true)
}

object Q5 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfQ5(argv)

    log.info("Input: " + args.input())

    val conf = new SparkConf().setAppName("Q5")
    val sc = new SparkContext(conf)

    // o_custkey -> o_orderkey
    val orders = sc.textFile(args.input()+"/orders.tbl")
    val ordersData = orders
      .map(line => {
        val params = line.split("""\|""")
        (params(1), params(0))
      })

    // c_custkey -> c_nationkey
    val customer = sc.textFile(args.input()+"/customer.tbl")
    val customerData = customer
      .filter(line => {
        val nationkey = line.split("""\|""")(3)
        nationkey == "3" || nationkey == "24"
      })
      .map(line => {
        val params = line.split("""\|""")
        (params(0), params(3))
      })

    // orderkey -> nationkey
    val orderToNation = ordersData
      .cogroup(customerData)
      .flatMap(line => {
        if (!line._2._1.isEmpty && !line._2._2.isEmpty) {
          line._2._1.map(orderkey => (orderkey, line._2._2.mkString)).toList
        } else {
          List()
        }
      })

    // orderkey -> date + orderkey -> nationkey
    val lineitem = sc.textFile(args.input()+"/lineitem.tbl")
    val lineitemData = lineitem
      .map(line => {
        val params = line.split("""\|""")
        (params(0), params(10).substring(0,7))
      })
      .cogroup(orderToNation)
      .flatMap(line => {
        if (!line._2._1.isEmpty && !line._2._2.isEmpty) {
          val nationkey = line._2._2.mkString
          line._2._1.map(date => (nationkey + ":" + date, 1)).toList
        } else {
          List()
        }
      })
      .reduceByKey(_ + _)
      .sortByKey(true)
      .map(line => {
        val country = if (line._1.split(":")(0) == "24") "UNITED STATES" else "CANADA"
        (country, line._1.split(":")(1), line._2)
      })
      .collect()

    for (x <- lineitemData) {
      println(x)
    }
  }
}
