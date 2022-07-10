package com.hitsz.recommender
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


//样例类:商品数据集
case class Product (productId: Int, name: String, imageUrl:String,categories:String,tags:String)

//评分数据集
case class Rating(userId:Int,ProductId:Int,score:Double,timestamp:Int)

//MongoDB的相关配置样例类

/**
 * MongoDB连接配置
 * @param uri：MongoDB连接的URI
 * @param db：要操作的DB
 */
case class MongoConfig(uri:String,db:String)
object DataLoader {
  //定义数据文件路径
  val PRODUCT_DATA_PATH = "E:\\Big_data\\recommendation_system\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\products.csv"
  val RATING_DATA_PATH = "E:\\Big_data\\recommendation_system\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  //定义MongoDB中存储的表名
  val MONGODB_PRODUCT_COLLECTION = "Product"
  val MONGODB_RATING_COLLECTION = "Rating"
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
    //加载数据：通过spark从文件中读取原数据
    //用sparkSession里面的一个方法可能传过来的就是一个RDD，后续存DataFream的时候，也需要RDD进行一个转换
    val productRDD = spark.sparkContext.textFile(PRODUCT_DATA_PATH)
    //转换成DataFream
    val productDF = productRDD.map(item => {
      val attr = item.split("\\^")  //正则的一个表示  。。Product数据通过^分隔，切分出来
      //转换成Product
      Product(attr(0).toInt,attr(1).trim,attr(4).trim,attr(5).trim,attr(6).trim)
    }).toDF()


    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH);
    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    //定义一个隐式变量，防止后续频繁传入mongo的连接参数
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    storeDataInMongoDB(productDF,ratingDF);   //将数据存到MongoDB中
    spark.stop();
  }

  def storeDataInMongoDB(prductDF: DataFrame, ratingDF: DataFrame)(implicit mongoConfig: MongoConfig):Unit = {
    //新建一个mongoDB的连接,客户端
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    //定义要操作的mongodb的表
    val productCollection = mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION)
    val ratingCollection = mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)

    //如果表存在，直接删掉
    productCollection.dropCollection()
    ratingCollection.dropCollection()

    //将当前数据存入对应的表中
    prductDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_PRODUCT_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    ratingDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //对表创建索引
    productCollection.createIndex(MongoDBObject("productId" -> 1))
    ratingCollection.createIndex(MongoDBObject("productId" -> 1))
    ratingCollection.createIndex(MongoDBObject("userId" -> 1))
    mongoClient.close()
  }
}
