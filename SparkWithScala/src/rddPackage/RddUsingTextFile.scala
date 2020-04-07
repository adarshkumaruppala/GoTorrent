package rddPackage

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object RddUsingTextFile {
  def main(args : Array[String]):Unit={
    val sparkConf=new SparkConf()
                  .setMaster("local")
                  .setAppName("spark with scala using text file")
   
   val sc=new SparkContext(sparkConf)
    val file="C:/Users/my/Downloads/ghtorrent-logs.txt"
    val textRdd=sc.textFile(file)
    val first=textRdd.first()
   print(first)
    //print(textRdd.count()+"  ")
   // textRdd.take(10).foreach(println)
      val counts=textRdd.flatMap(line=>line.split(",")).map(word=>(word,1))
       counts.take(4).foreach(println)
       
  }
}