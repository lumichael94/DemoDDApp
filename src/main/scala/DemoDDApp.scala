import org.apache.spark._
import com.datastax.spark.connector._
import java.security.MessageDigest
import scala.util.hashing.MurmurHash3

case class intermed(uuid: String, hash: Int)

object DemoDDApp{

  def stringHash(str:String, seed:Int): Int = {
    //Just to kill some more time.
    val md = MessageDigest.getInstance("SHA-256")
    //Creates a bytes array
    val byteArr = md.update(str.getBytes("UTF-8"))
    MurmurHash3.bytesHash(md.digest, seed)
  }

  def arrayHash(arr:Array[Int], seed:Int): Int = {
    MurmurHash3.arrayHash(arr, seed)
  }

  def computeRow(row: CassandraRow): Int ={
    val seed = row.get[Int]("address")

    val uuid = stringHash(row.get[String]("hash"), seed)
    val email = stringHash(row.get[String]("email"), seed)
    val phonenumber = stringHash(row.get[String]("phonenumber"), seed)
    val username = stringHash(row.get[String]("username"), seed)

    val arr = Array(uuid, email, phonenumber, username)
    arrayHash(arr, seed) 
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DemoDDApp")

    val sc = new SparkContext(conf)
    
    val md = MessageDigest.getInstance("MD5")
    //Create RDD from table
    val random1 = sc.cassandraTable("test", "random1")
    //val table2 = sc.cassandraTable[(String, String)]("test", "random2")
    //val table3 = sc.cassandraTable[(String, String)]("test", "random3")
    //val table4 = sc.cassandraTable[(String, String)]("test", "random4")

    //Iterate through the rows.
    //Apply the hashing function, map UUID to new hash
    //Save to new RDD
    val middle1 = random1.map{r =>
      intermed(r.get[String]("hash"), computeRow(r))
    }

    middle1.saveAsCassandraTable("test", "middle1", SomeColumns("uuid", "hash"))


    sc.stop;

  }
}
