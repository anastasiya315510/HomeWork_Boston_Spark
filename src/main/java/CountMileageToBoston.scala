import org.apache.spark.rdd.RDD
/*
Counting the biggest mileage to Boston
*/
class CountMileageToBoston {
  def mileage(rdd:RDD[String]): Any={
    rdd.filter(_.toLowerCase.contains("boston"))             //every line that contains Boston
      .map(_.replaceFirst("\\d{3}",""))   //cut first 3 digit ids
      .map(str=>str.substring(0, str.lastIndexOf(" "))) //cut date
      .map(_.replaceAll("[^\\d.]",""))    // leave in stream just digits value
      .map(_.toInt)                                          //convert to int
      .sum()

  }

}
