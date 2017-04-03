package com.sihanwang.spark.scala.study

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a +""+ b)
  
  def main(args : Array[String]) {
    println("concat arguments = " + foo(args))
    
    
    
    
    
  }

}
