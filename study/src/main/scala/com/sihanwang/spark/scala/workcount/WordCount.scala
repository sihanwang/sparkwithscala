package com.sihanwang.spark.scala.workcount

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

//http://nishutayaltech.blogspot.com/2015/04/how-to-run-apache-spark-on-windows7-in.html
//https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-tips-and-tricks-running-spark-windows.html


//export HBASE_HOME=/hadoop/cloudera/parcels/CDH-5.5.2-1.cdh5.5.2.p1699.1674/


//export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:$($HBASE_HOME/bin/hbase classpath)"

//spark-submit --class com.sihanwang.spark.scala.study.WordCount --master yarn-cluster --deploy-mode cluster study-0.0.1-SNAPSHOT-jar-with-dependencies.jar "testdata/vtlocation.log" "testdata/result.txt"
//spark-submit.cmd --class com.sihanwang.spark.scala.study.WordCount --master local[*]  C:\github\sparkwithscala\study\target\study-0.0.1-SNAPSHOT-jar-with-dependencies.jar file:///c:\\test\\vtlocation.log file:///c:\\test\\result.txt

object WordCount extends App {
  def ReceivedParameters = args.mkString("<", ",", ">")

  println(ReceivedParameters);

  var sourcefile: String = "";
  var Resultfile: String = "";

  args.length match {
    case x: Int if x == 2 =>
      sourcefile = args(0); Resultfile = args(1)
    case _                => println("Please input sourcefile/Resultfile");
  }

  val conf = new SparkConf().setAppName("WordCount")
  val sc = new SparkContext(conf)

  val input = sc.textFile(sourcefile)
  
  println(input.collect().mkString("<", ",", ">"))

  val words = input.flatMap(line => line.split(" "))
  
  println(words.collect().mkString("<", ",", ">"))

  val counts = words.map(word => (word, 1)).reduceByKey((x, y) => x + y)
  
  println(counts.collect().mkString("<", ",", ">"))

  counts.saveAsTextFile(Resultfile)

}