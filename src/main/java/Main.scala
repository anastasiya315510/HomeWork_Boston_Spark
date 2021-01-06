import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Main {
  def main(args: Array[String]): Unit = {
  val spark =new SparkContext("local", "master");
    val rdd: RDD[String] = spark.textFile("src/main/resources/taxi_orders.txt");
    val rdd_2: RDD[String] = spark.textFile("src/main/resources/drivers.txt");
    val count = new DriverChampions;
    count.getBests(rdd, rdd_2)







  }

}
