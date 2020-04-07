package normalScala

object caseClass {
  def main(args:Array[String])={
    val a=Employee(1,"adarsh",22)
    print(a.id)
    
  }
}
case class Employee(id :Int,name :String,age :Int)