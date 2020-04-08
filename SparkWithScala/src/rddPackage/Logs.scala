package rddPackage

import java.util.Date

case class Logs(debug_Lev :String,time :Date,download_Id:Int,
                   retrival_Stage:String,rest:String)