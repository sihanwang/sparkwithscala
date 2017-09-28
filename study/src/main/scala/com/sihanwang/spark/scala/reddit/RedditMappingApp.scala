package com.sihanwang.spark.scala.reddit

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Milliseconds, Seconds, StreamingContext }
import org.apache.hadoop.io.{ Text, LongWritable, IntWritable }
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.streaming.dstream.DStream
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.hadoop.mapreduce.lib.output.{ TextOutputFormat => NewTextOutputFormat }
import org.apache.spark.streaming.dstream.PairDStreamFunctions
import org.apache.log4j.LogManager
import org.json4s._
import org.json4s.native.JsonMethods._
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.HashPartitioner

////spark-submit --class com.sihanwang.spark.scala.reddit.RedditMappingApp --master local[*] lib\study-0.0.1-SNAPSHOT-jar-with-dependencies.jar RedditMappingApp file:///C:\\spark-2.1.1-bin-hadoop2.7\\bin\\lib\\input file:///C:\\test\\result
object RedditMappingApp {

    def main(args : Array[String]) {
        if (args.length != 3) {
            System.err.println(
                "Usage: RedditMappingApp <appname> <input_path> <output_path>")
            System.exit(1)
        }
        val Seq(appName, inputPath, output_path) = args.toSeq
        val LOG = LogManager.getLogger(this.getClass)

        val conf = new SparkConf()
            .setAppName(appName)
            .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

        val ssc = new StreamingContext(conf, Seconds(15))
        LOG.info("Started at %d".format(ssc.sparkContext.startTime)) //可以使用log在运行driver的控制台上输出

        val comments = ssc.fileStream[LongWritable, Text, TextInputFormat](inputPath, (f : Path) => true, newFilesOnly = false).map(pair => pair._2.toString)

        LOG.info("ssc.textFileStream:" + inputPath)

        comments.saveAsTextFiles(output_path, "comments") //saveAsTextFiles可以作为打印中间结果的调试手段

        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val tsKey = "created_utc"
        val secs = 1000L
        val keyedByDay = comments.map(rec => {
            val ts = (parse(rec) \ tsKey).values
            (sdf.format(new Date(ts.toString.toLong * secs)), rec)
        })

        keyedByDay.saveAsTextFiles(output_path, "keyedByDay")

        val keyedByDayPart = comments.mapPartitions(iter => { //对每个partition进行map
            var ret = List[(String, String)]()
            while (iter.hasNext) {
                val rec = iter.next
                val ts = (parse(rec) \ tsKey).values
                ret.::=(sdf.format(new Date(ts.toString.toLong * secs)), rec) //::=可能是list的追加操作
            }
            ret.iterator
        })

        keyedByDayPart.saveAsTextFiles(output_path, "keyedByDayPart")

        val wordTokens = comments.map(rec => {
            ((parse(rec) \ "body")).values.toString.split(" ").mkString("[", ",", "]")
        })

        wordTokens.saveAsTextFiles(output_path, "wordTokens")

        val wordTokensFlat = comments.flatMap(rec => {
            ((parse(rec) \ "body")).values.toString.split(" ")
        })

        wordTokensFlat.saveAsTextFiles(output_path, "wordTokensFlat")

        val filterSubreddit = comments.filter(rec =>
            (parse(rec) \ "subreddit").values.toString.equals("AskReddit"))

        filterSubreddit.saveAsTextFiles(output_path, "filterSubreddit")

        val sortedByAuthor = comments.transform(rdd =>
            (rdd.sortBy(rec => (parse(rec) \ "author").values.toString))) //transform将一个RDD转换为另一个RDD

        sortedByAuthor.saveAsTextFiles(output_path, "sortedByAuthor")

        val merged = comments.union(comments)
        merged.saveAsTextFiles(output_path, "merged") //变成两个partition了

        val recCount = comments.count()
        recCount.saveAsTextFiles(output_path, "recCount") //分区突然增加为4个，只有第一个分区的输出文件有一条记录说明记录的个数

        val recCountValue = comments.countByValue()
        recCountValue.saveAsTextFiles(output_path, "recCountValue") ////分区突然增加为4个, 返回(record,1)

        val totalWords = comments.map(rec => ((parse(rec) \ "body").values.toString))
            .flatMap(body => body.split(" "))
            .map(word => 1)
            .reduce(_ + _) //只有一个分区结果文件，包含一条记录521(单词的总数)

        totalWords.saveAsTextFiles(output_path, "totalWords")

        val topAuthors = comments.map(rec => ((parse(rec) \ "author").values.toString, 1))
            .groupByKey() //groupbykey
            .map(r => (r._2.sum, r._1))
            .transform(rdd => rdd.sortByKey(ascending = false)) //自动返回4个分区的结果文件，排序靠前的记录包含在第一个分区，最少的在第四个分区

        topAuthors.saveAsTextFiles(output_path, "topAuthors")

        val topAuthors2 = comments.map(rec => ((parse(rec) \ "author").values.toString, 1))
            .reduceByKey(_ + _) //reducebykey
            .map(r => (r._2, r._1))
            .transform(rdd => rdd.sortByKey(ascending = false))

        topAuthors2.saveAsTextFiles(output_path, "topAuthors2") //自动返回4个分区的结果文件，排序靠前的记录包含在第一个分区，最少的在第四个分区

        val topAuthorsByAvgContent = comments.map(rec => ((parse(rec) \ "author").values.toString, (parse(rec) \ "body").values.toString.split(" ").length))
            .combineByKey(
                (v) => (v, 1),
                (accValue : (Int, Int), v) => (accValue._1 + v, accValue._2 + 1),
                (accCombine1 : (Int, Int), accCombine2 : (Int, Int)) => (accCombine1._1 + accCombine2._1, accCombine1._2 + accCombine2._2),
                new HashPartitioner(ssc.sparkContext.defaultParallelism))
            .map({ case (k, v) => (k, v._1 / v._2.toFloat) })
            .map(r => (r._2, r._1))
            .transform(rdd => rdd.sortByKey(ascending = false)) //排序每个读者的评论的平均长度

        ssc.start()
        ssc.awaitTermination()

    }
}