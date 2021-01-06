import org.apache.spark.rdd.RDD
/*
Counting Boston trips where mileage more than 10 km
 */
class CountBostonTrips {
def whereWayMoreThan10Km(rdd:RDD[String]): Long ={
     rdd
    .filter(str=>str.toLowerCase.contains("boston"))               //choose every digit that contains Boston
    .map(str=>str.replaceFirst("\\d{3}", " ") ) //delete all digits ids
    .map(str=>str.substring(0, str.lastIndexOf(" ")))         //delete all dates
    .map(str=>str.replaceAll("[^\\d.]", ""))    //delete all words that are not digits
    .map(str=>str.toInt)                                           //convert every string to digit
    .filter(_>10)                                                  //select digits bigger than 10
    .count()



}
}
