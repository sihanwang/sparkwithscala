package com.sihanwang.spark.scala.study

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

//export HBASE_HOME=/hadoop/cloudera/parcels/CDH-5.5.2-1.cdh5.5.2.p1699.1674/
//export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:$($HBASE_HOME/bin/hbase classpath)"

//spark-submit --class com.sihanwang.spark.scala.study.WordCount --master yarn-cluster --deploy-mode cluster --queue commoditiesdata study-0.0.1-SNAPSHOT.jar "jing.wang/README.md" "jing.wang/result.txt"
//spark-submit --principal bigdata-app-commoditiesdata-pcadmin@INTQA.THOMSONREUTERS.COM --keytab bigdata-app-commoditiesdata-pcadmin.keytab --class com.tr.cdb.distribution.series.sdi.job.CDBSeries_SDI_Producer --master yarn-cluster --deploy-mode cluster --queue commoditiesdata CDBSeries_SDI-0.0.1-SNAPSHOT.jar F 00000000000000 20170101000000 /config/hbase-site-titan.xml /config/core-site.xml 

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