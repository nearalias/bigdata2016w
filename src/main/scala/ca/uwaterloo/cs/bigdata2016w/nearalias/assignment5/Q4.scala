package ca.uwaterloo.cs.bigdata2016w.nearalias.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

class ConfQ4(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "l_shipdate predicate", required = true)
}

object Q4 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfQ4(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q4")
    val sc = new SparkContext(conf)

    val date = args.date()

    val orders = sc.textFile(args.input()+"/orders.tbl")
    val ordersData = orders
      .map(line => {
        val params = line.split("""\|""")
        (params(0), params(1))
      })

    val customer = sc.textFile(args.input()+"/customer.tbl")
    val customerData = customer
      .map(line => {
        val params = line.split("""\|""")
        (params(0), params(3))
      })

    val nation = sc.textFile(args.input()+"/nation.tbl")
    val nationData = nation
      .map(line => {
        val params = line.split("""\|""")
        (params(0), params(1))
      })

    val lineitem = sc.textFile(args.input()+"/lineitem.tbl")
    val lineitemData = lineitem
      .filter(line => { line.split("""\|""")(10).startsWith(date) })
      .map(line => (line.split("""\|""")(0), 0) )
      .cogroup(ordersData)
      .flatMap(line => {
        if (!(line._2._1.isEmpty) && !(line._2._2.isEmpty)) {
          val custkey = line._2._2.mkString
          line._2._1.map(zero => (custkey, 0)).toList
        } else {
          List()
        }
      })
      .cogroup(customerData)
      .flatMap(line => {
        if (!(line._2._1.isEmpty) && !(line._2._2.isEmpty)) {
          val nationkey = line._2._2.mkString
          line._2._1.map(zero => (nationkey, 0)).toList
        } else {
          List()
        }
      })
      .cogroup(nationData)
      .flatMap(line => {
        if (!(line._2._1.isEmpty) && !(line._2._2.isEmpty)) {
          line._2._1.map(zero => ((line._1.toInt, line._2._2.mkString), 1)).toList
        } else {
          List()
        }
      })
      .reduceByKey(_ + _)
      .sortBy(_._1)
      .map(line => (line._1._1, line._1._2, line._2))
      .collect()

    for (x <- lineitemData) {
      println(x)
    }
  }
}
