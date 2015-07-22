import org.apache.spark._
import com.datastax.spark.connector._
import java.security.MessageDigest
import scala.util.hashing.MurmurHash3
import org.apache.spark.rdd.RDD

case class intermed(uuid: String, hash: Int) 
case class finalTable(uuid: String, hash: Int)

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

  def randToMid(rdd: RDD[CassandraRow]): RDD[CassandraRow]={
    val middle = rdd.map{r =>
      val uuid = r.get[String]("hash")
      val hash = computeRow(r)
      CassandraRow.fromMap(Map("uuid" -> uuid, "hash" -> hash))
    }
    return middle
  }

  //Yeah...this is just a reduce function. Rewriting
  def combineTwo(rdd1: RDD[CassandraRow], rdd2: RDD[CassandraRow]): RDD[CassandraRow]={
    
    val finale = rdd1.flatMap{r1 =>
        rdd2.map{r2 => 
        val row1UUID= r1.get[String]("uuid")
        val row2UUID= r2.get[String]("uuid")
        val row1Hash= r1.get[Int]("hash")
        val row2Hash= r1.get[Int]("hash")

        val hash1 = stringHash(row1UUID, row2Hash)
        
        val uuid = Integer.toHexString(hash1)
        val hash = stringHash(row2UUID, row1Hash)
        CassandraRow.fromMap(Map("uuid" -> uuid, "hash" -> hash))
      }.collect
    }
    return finale;
  }
  def zap(rdd1: RDD[CassandraRow], rdd2: RDD[CassandraRow]): RDD[(CassandraRow, CassandraRow)]={

    val indexedRDD1 = rdd1.zipWithIndex.map { case (v, i) => i -> v }
    val indexedRDD2 = rdd2.zipWithIndex.map { case (v, i) => i -> v }
    val combined = indexedRDD1.leftOuterJoin(indexedRDD2).map {
      case (i, (v1, Some(v2))) =>  (v1, v2)
    }
    return combined
  }
  //Actual reduce
  def reduceTwo(rdd1: RDD[CassandraRow], rdd2: RDD[CassandraRow]): RDD[CassandraRow]={
    val zip = zap(rdd1, rdd2)
    //val asdf = zip.get[String]("uuid")
    val response = zip.map{x =>
        val row1UUID= x._1.get[String]("uuid")
        val row2UUID= x._2.get[String]("uuid")
        val row1Hash= x._1.get[Int]("hash")
        val row2Hash= x._2.get[Int]("hash")

        val hash1 = stringHash(row1UUID, row2Hash)
        
        val uuid = Integer.toHexString(hash1)
        val hash = stringHash(row2UUID, row1Hash)
        CassandraRow.fromMap(Map("uuid" -> uuid, "hash" -> hash))
    }
    return response
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
    val middle3 = randToMid(random3)
    val middle4 = randToMid(random4)


    val final1 = reduceTwo(middle1, middle3)
    val final2 = reduceTwo(middle2, middle4)

    val finalRDD = reduceTwo(final1, final2)

    val finalRow = finalRDD.collect.map{row =>
      val x = row.get[String]("uuid") 
      val y = row.get[Int]("hash") 
      stringHash(x,y)
    }
    val finalHash = MurmurHash3.arrayHash(finalRow) 
    println("\n\n\nThe hash is: " + finalHash + "\n\n\n")
    sc.stop

  }
}
