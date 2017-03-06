package xyz.eugenzyx

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext

object App {
  case class Contact(name: String, phone: String, something: Int)
  case class Person(name: String, age: Int, contacts: Seq[Contact])

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    val contacts = (1 to 100).map(i => Contact(null, i.toString, null))

    sc.parallelize(contacts).toDF().write.mode(SaveMode.Overwrite).format("orc").save(s"hdfs://${ args(0) }/tmp/people_null")
  }
}
