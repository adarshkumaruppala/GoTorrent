package rddPackage

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Date
import java.text.SimpleDateFormat

case class LogLine(debug_level :String,timestamp :Date,download_id:Int,
                   retrival_stage:String,rest:String)
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
         new Some(LogLine(debug_level, df.
         parse(dateTime.replace("T", ":")), downloadId.toInt, retrievalStage, rest))
    case _ => None;
              
            }
            )
   //how many lines does RDD contain 
            
   println(rdd.count())
   
   // count the number of Warning messages in log file
   
   println(rdd.filter(x => x.debug_level=="WARN").count())
   
   //how many repositories where processed in total  "use api_client"
   
   val repos=rdd.filter(_.retrival_stage == "api_client")
             .map(_.rest.split("/")
             .slice(4,6)
             .mkString("/")
             .takeWhile(_!='?'))
             
   println(repos.distinct().count())
   
   /*// whivh client did most Http Requests
   
   println(rdd.filter(_.retrival_stage =="api_client")
              .keyBy(_.download_id)
              .mapValues(1 =>1)
              .reduceByKey(_+_)
              .sortByKey(x=>x._2,false)
              .take(3))*/
   
              
              
            
     
    
    
   }
}