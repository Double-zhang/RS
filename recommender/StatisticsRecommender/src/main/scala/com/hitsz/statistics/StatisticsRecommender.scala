package com.hitsz.statistics
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

//评分数据集
case class Rating(userId:Int,ProductId:Int,score:Double,timestamp:Int)
/**
 * MongoDB连接配置
 * @param uri：MongoDB连接的URI
 * @param db：要操作的DB
 */
case class MongoConfig(uri:String,db:String)

object StatisticsRecommender {
  val MONGODB_RATING_COLLECTION = "Rating"

  //统计的表的名称
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  val AVERAGE_PRODUCTS = "AverageProducts"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017//recommender",
      "mongo.db" -> "recommender"

    )

    //创建一个spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    //创建一个spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._  //为了操作dataFream，dataSet等

    //定义一个隐式变量，防止后续频繁传入mongo的连接参数
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    //加载数据
    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongo.spark.sql")
      .load().as[Rating]
      .toDF()

    //创建一张ratings的临时表
    ratingDF.createOrReplaceTempView("ratings")

    //TODO:用spark sql做不同的统计推荐

    //1.历史热门商品:productId,count
    val rateMoreProductsDF = spark.sql("select productId,count(productId) as count from ratings group by productId order by count")
    storeDFToMongoDB(rateMoreProductsDF,RATE_MORE_PRODUCTS)  //存储到mongo中的哪个表中
    //2.近期热门统计
    //创建一个日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    //注册UDF，将timestamp转化为yyyyMM格式
    spark.udf.register("changeDate",(x,Int) => simpleDateFormat(new Date(x * 1000L)).toInt)
    //把原始rating数据转换成想要的结构  productId,score,yearmonth
    val ratingOfYearMonthDF = spark.sql("select productId,score,changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonthDF.createOrReplaceTempView("ratingOfMonth")
    val rateMoreRecentlyProductsDF = spark.sql("select productId,count(productId) as count, yearmonth from ratingOfMonth group by yearmonth order by yearmonth desc, count desc")
    //把df保存到mongodb中
    storeDFToMongoDB(rateMoreRecentlyProductsDF,RATE_MORE_RECENTLY_PRODUCTS)

    //3.优质商品统计
    val averageProductsDF = spark.sql("select productId, avg(score) as avg from ratings group by productId order by avg desc")
    storeDFToMongoDB( averageProductsDF, AVERAGE_PRODUCTS )

    spark.stop()
  }

  def storeDFToMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri",mongoConfig.uri)
      .option("collection",collection_name)
      .mode("overwrite")
      .format("com.mongo.spark.sql")
      .save()
  }
}
