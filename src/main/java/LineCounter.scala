import org.apache.spark.rdd.RDD

case class LineCounter(){
  def countLines(rdd:RDD[String]): Long ={
    rdd.count()

  }
}
