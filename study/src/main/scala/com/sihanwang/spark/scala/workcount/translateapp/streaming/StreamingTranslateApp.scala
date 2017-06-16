package com.sihanwang.spark.scala.workcount.translateapp.streaming


import scala.io.Source
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

//spark-submit --class com.sihanwang.spark.scala.workcount.translateapp.streaming.StreamingTranslateApp --master local[*] study-0.0.1-SNAPSHOT-jar-with-dependencies.jar jingtranslation file:///C:\\spark-2.1.1-bin-hadoop2.7\\bin\\lib\\input\\ file:///C:\\test\\result German
object StreamingTranslateApp {
  def main(args: Array[String]) {
      if (args.length !=4)
      {
          System.err.println("Usage: StreamingTranslateApp <appname> <book_path> <output_path> <language>")
          System.exit(1)
      }
      
      
      val Seq(appName, bookPath, outputPath, lang) = args.toSeq
      
      val dict=getDictionary(lang)
      
      val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)
      
      val ssc = new StreamingContext(conf, Seconds(1))
      
      val book = ssc.textFileStream(bookPath)
      
      val translated = book.map(line => line.split("\\s+"). //"\\s+" 匹配任何空白字符，包括空格、制表符、换页符等等
              map(word => dict.getOrElse(word,word)).mkString(" "))
      
      translated.saveAsTextFiles(outputPath)
      
      ssc.start()
      ssc.awaitTermination()
      
  }
  
  def getDictionary(lang: String): Map[String, String] = {
      if (!Set("German","French","Italian","Spanish").contains(lang))
      {
          System.err.println("Unsupported language: %s".format(lang))
          System.exit(1)
      }
      
      val url="http://www.june29.com/IDP/files/%s.txt".format(lang)
      
      println("Grabbing dictionary from: %s".format(url))
      
      Source.fromURL(url, "ISO-8859-1").mkString
      .split("\\r?\\n") //\\r?\\n 匹配换行符
      .filter(line => !line.startsWith("#"))
      .map(line => line.split("\\t"))  //\\t匹配制表符
      .map(tkns => (tkns(0).trim, tkns(1).trim)).toMap
      
  }
}