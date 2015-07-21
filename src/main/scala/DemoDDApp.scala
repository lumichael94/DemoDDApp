import org.apache.spark._
import com.datastax.spark.connector._

object DemoDDApp{
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DemoDDApp")

    val sc = new SparkContext(conf)

    val table1 = sc.cassandraTable[(String, String)]("test", "random")
    //val table2 = sc.cassandraTable[(String, String)]("test", "random2")
    //val table3 = sc.cassandraTable[(String, String)]("test", "random3")
    //val table4 = sc.cassandraTable[(String, String)]("test", "random4")

    val count = table1.count;
    sc.stop;
    println(count);
  }
}
