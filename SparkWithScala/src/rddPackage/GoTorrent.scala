package rddPackage

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Date
import java.text.SimpleDateFormat
object GoTorrent {
   def main(args : Array[String]):Unit={
      val sparkConf=new SparkConf()
                  .setMaster("local")
                  .setAppName("spark with scala using text file")
     val dateFormat = "yyyy-MM-dd:HH:mm:ss"
     val regex = """([^\s]+), ([^\s]+)\+00:00, ghtorrent-([^\s]+) -- ([^\s]+).rb: (.*$)""".r
     val sc=new SparkContext(sparkConf)
     val file="C:/Users/my/Downloads/ghtorrent-logs.txt"
     val rdd=sc.textFile(file).flatMap(x => x match{
      
     case regex(debug_level, dateTime, downloadId,
         retrievalStage, rest) => val df=new SimpleDateFormat(dateFormat)
         new Some(Logs(debug_level, df.
         parse(dateTime.replace("T", ":")), downloadId.toInt, retrievalStage, rest))
     case _ => None;
              
            }
            )
   //  1)  how many lines does RDD contain 
            
    println(rdd.count())
   
   //  2)  count the number of Warning messages in log file
   
    println(rdd.filter(x => x.debug_Lev=="WARN").count())
   
   //  3)  how many repositories where processed in total  "use api_client"
   
    val repos=rdd.filter(_.retrival_Stage == "api_client")
             .map(_.rest.split("/")
             .slice(4,6)
             .mkString("/")
             .takeWhile(_!='?'))
             
    println(repos.distinct().count())
   
   //  4) whivh client did most Http Requests
   
   println(rdd.filter(_.retrival_Stage =="api_client")
              .keyBy(_.download_Id)
              .mapValues(x=>1)
              .reduceByKey(_+_)
              .sortBy(x=>x._2,false)
              .take(3))
   
    // 5)  which client did most failed Http Requessts
              
     println(rdd.filter(_.retrival_Stage=="api_client")
                .filter(_.rest.startsWith("Failed"))
                .keyBy(_.download_Id)
                .mapValues(x=>1)
                .reduceByKey(_+_)
                .sortBy(x=>x._2, false)
                .take(3))
              
            
     
    
    
   }
}