package ca.uwaterloo.cs.bigdata2016w.nearalias.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

class ConfQ7(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "l_shipdate predicate", required = true)
}

object Q7 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfQ7(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q7")
    val sc = new SparkContext(conf)

    val date = args.date()

    // o_orderkey -> (o_custkey, o_orderdate, o_shippriority)
    val orders = sc.textFile(args.input()+"/orders.tbl")
    val ordersData = orders
      .filter(line => { line.split("""\|""")(4) < date })
      .map(line => {
        val params = line.split("""\|""")
        (params(0), (params(1), params(4), params(7)))
      })

    // l_orderkey -> (l_extendedprice, l_discount)
    val lineitem = sc.textFile(args.input()+"/lineitem.tbl")
    val lineitemData = lineitem
      .filter(line => { line.split("""\|""")(10) > date })
      .map(line => {
        val params = line.split("""\|""")
        (params(0), (params(0), params(5).toDouble, params(6).toDouble))
      })

    // c_custkey -> c_name
    val customer = sc.textFile(args.input()+"/customer.tbl")
    val customerData = customer
      .map(line => {
        val params = line.split("""\|""")
        (params(0), params(1))
      })
    val customerMap = sc.broadcast(customerData.collectAsMap())

    val result = ordersData
      .cogroup(lineitemData)
      .flatMap(line => {
        if (!line._2._1.isEmpty && !line._2._2.isEmpty) line._2._1.map(order => (order, line._2._2)).toList else List()
      })
      .flatMap(line => {
        val custkey = line._1._1
        val orderdate = line._1._2
        val shippriority = line._1._3
        val cMap = customerMap.value
        val name = cMap.get(custkey).get
        line._2.map(item => {
          val revenue = item._2 * (1 - item._3)
          (name +":"+ item._1 +":"+ orderdate +":"+ shippriority, revenue)
        }).toList
      })
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .map(line => {
        val keys = line._1.split(":")
        (keys(0), keys(1), line._2, keys(2), keys(3))
      })
      .take(10)

    for (x <- result) {
      println(x)
    }
  }
}
