import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * https://www.cnblogs.com/ronnieyuan/p/12249118.html
 * Spark-Sql参考这篇
 */
object SparkSqlApplication {
  case class Person(name:String, age:Int, addr:String)

  def test(args: Array[String]): Unit = {
    val reader: SparkSession = SparkSession.builder().appName("CSV Reader").master("local[*]").getOrCreate()
    val civic_info: DataFrame = reader.read.format("csv").option("delimiter",",").option("header", "true").option("nullValue", "\\N").option("inferSchema","true").load("D:\\IdeaProjects\\csv2sql\\src\\main\\resources\\civic_info.csv")
    civic_info.show()
    civic_info.printSchema()
    val ticket_info: DataFrame = reader.read.format("csv").option("delimiter",",").option("header", "true").option("nullValue", "\\N").option("inferSchema","true").load("D:\\IdeaProjects\\csv2sql\\src\\main\\resources\\ticket_info.csv")
    ticket_info.show()
    civic_info.printSchema()
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Sql Test").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext._
    import sqlContext.implicits._

    val people = sc.textFile("D:\\IdeaProjects\\csv2sql\\src\\main\\resources\\data1.txt").map(_.split(",")).map( p => Person(p(0),p(1).trim.toInt,p(2))).toDF()

    people.registerTempTable("people")

    val teenagers = sql("SELECT name, age, addr FROM people ORDER BY age")

    teenagers.map( t => "name:" + t(0) + " age:" + t(1) + " addr:" + t(2)).collect().foreach(println)

    sc.stop();
  }


}
