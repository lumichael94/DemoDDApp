import org.apache.spark._
import com.datastax.spark.connector._
import java.security.MessageDigest
import scala.util.hashing.MurmurHash3
import org.apache.spark.rdd.RDD

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

  def randToMid(rdd: RDD[CassandraRow]): RDD[intermed]={
    val middle1 = rdd.map{r =>
      intermed(r.get[String]("hash"), computeRow(r))
    }
    return middle1
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DemoDDApp")

    val sc = new SparkContext(conf)
    
    //Create RDD from table
    val random1 = sc.cassandraTable("test", "random1")
    val random2 = sc.cassandraTable("test", "random2")
    val random3 = sc.cassandraTable("test", "random3")
    val random4 = sc.cassandraTable("test", "random4")

    //Iterate through the rows.
    //Apply the hashing function, map UUID to new hash
    //Save to new RDD
    val middle1 = randToMid(random1)
    val middle2 = randToMid(random2)
    val middle3 = randToMid(random1)
    val middle4 = randToMid(random1)

    

    sc.stop;

  }
}
