package xyz.eugenzyx

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext

object App {
  case class Contact(name: String, phone: String)
  case class Person(name: String, age: Int, contacts: Seq[Contact])

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    val records = (1 to 100).map { i =>
      Person(s"name_$i", i, (0 to 1).map { m => Contact(s"contact_$m", s"phone_$m") })
    }

    sc.parallelize(records).toDF().write.format("orc").save("hdfs:///home/mapr/people")
  }
}
