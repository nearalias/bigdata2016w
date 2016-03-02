package ca.uwaterloo.cs.bigdata2016w.nearalias.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

class ConfQ3(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "l_shipdate predicate", required = true)
}

object Q3 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfQ3(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q3")
    val sc = new SparkContext(conf)

    val date = args.date()

    val part = sc.textFile(args.input()+"/part.tbl")
    val partData = part
      .map(line => {
        val params = line.split("""\|""")
        (params(0), params(1))
      })
    val partMap = sc.broadcast(partData.collectAsMap())

    val supplier = sc.textFile(args.input()+"/supplier.tbl")
    val supplierData = supplier
      .map(line => {
        val params = line.split("""\|""")
        (params(0), params(1))
      })
    val supplierMap = sc.broadcast(supplierData.collectAsMap())

    val lineitem = sc.textFile(args.input()+"/lineitem.tbl")
    val lineitemResults = lineitem
      .filter(line => {
        val params = line.split("""\|""")
        val pMap = partMap.value
        val sMap = supplierMap.value
        params(10).startsWith(date) && pMap.contains(params(1)) && sMap.contains(params(2))
      })
      .map(line => {
        val params = line.split("""\|""")
        val pMap = partMap.value
        val sMap = supplierMap.value
        (params(0).toInt, (pMap.get(params(1)).get, sMap.get(params(2)).get))
      })
      .sortByKey(true)
      .map(line => (line._1, line._2._1, line._2._2))
      .take(20)

    for (x <- lineitemResults) {
      println(x);
    }
  }
}
