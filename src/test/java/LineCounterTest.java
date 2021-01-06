import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.junit.*;

public class LineCounterTest {
    private static SparkConf conf;
    private static JavaSparkContext context;
    private LineCounter counter;
    private CountBostonTrips trips;
    private CountMileageToBoston mileage;

    @BeforeClass
    public static void setUpClass() throws Exception {
        conf = new SparkConf().setMaster("local[*]").setAppName("hello spark");
        context = new JavaSparkContext(conf);
    }
    @Before
    public void setUp() throws Exception {
        counter = new LineCounter();
        trips = new CountBostonTrips();
        mileage = new CountMileageToBoston();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        context.close();
    }

    @Test
    public void countLines() {
        RDD<String> rdd = context.textFile("src/main/resources/taxi_orders.txt").rdd();
        Assert.assertEquals(counter.countLines(rdd), 21L);
    }

    @Test
    public void whereWayMoreThan10Km(){
        RDD<String> rdd = context.textFile("src/main/resources/taxi_orders.txt").rdd();
        Assert.assertEquals(trips.whereWayMoreThan10Km(rdd), 1L);

    }

    @Test
    public void mileage(){
        RDD<String> rdd = context.textFile("src/main/resources/taxi_orders.txt").rdd();
        Assert.assertEquals(mileage.mileage(rdd), 32.0);

    }
}