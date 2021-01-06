import org.apache.spark.rdd.RDD




class DriverChampions{

def getBests(rdd:RDD[String],rdd_2:RDD[String]): Any = {

   rdd
    .map(str => str.substring(0, str.lastIndexOf(" ")))   //delete dates
    .map(_.split(" "))                                  //split by space
    .map(l => Tuple2.apply(l.head, l.last))                    //create tuple id and mileage
    .groupByKey()                                              // group by id
    .mapValues(_.map(_.toInt).sum)                             //count summary mileage each of driver
    .sortBy(-_._2)                                             //sort by mileage in descending order
    .take(3)                                             // take three leaders
    .map(s=>{
     rdd_2.filter(str => str.contains(s._1))                    //get from rdd2 every driver with current id
       .map(str => str.concat(" mileage: " + s._2))        //add mileage to string
       .foreach(println(_))                                     //print everything

    })


}

}
