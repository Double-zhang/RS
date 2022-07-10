package com.hitsz.online

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis





//连接助手对象，建立到redis和Mongodb的连接
object ConnHelper extends Serializable {

  //lazy：用的时候才会做一个加载
  //连接redis
  lazy val jedis = new Jedis("localhost")
  //连接mongo
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://localhost:27017/recommender"))
}

case class MongoConfig(uri:String,db:String)

// 标准推荐
case class Recommendation(productId:Int, score:Double)

// 用户的推荐
case class UserRecs(userId:Int, recs:Seq[Recommendation])

//商品的相似度
case class ProductRecs(productId:Int, recs:Seq[Recommendation])

object OnlineRecommender{

  //定义表名
  //保存实时推荐的结果
  val STREAM_RECS = "StreamRecs"
  //从这里面读取相似度矩阵
  val MONGODB_PRODUCT_RECS_COLLECTION = "ProductRecs"
  val MAX_USER_RATINGS_NUM = 20
  //最大多少相似商品
  val MAX_SIM_PRODUCTS_NUM = 20
  val MONGODB_RATING_COLLECTION = "Rating"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )
    //创建一个SparkConf配置
    val sparkConf = new SparkConf().setAppName("OnlineRecommender").setMaster(config("spark.cores"))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    //spark1.0本身所有的东西都分开  spark2.0 中的session封装了sqlContext,hiveContext等，但是没有StreamingContext。所以这里要new StreamingContext
    val ssc = new StreamingContext(sc,Seconds(2))  //一个批次的计算间隔时间

    implicit val mongConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    import spark.implicits._

    // 广播 商品相似度矩阵
    //转换成为 Map[Int, Map[Int,Double]]
    //根据第一个pid1，找到pid1 的相似度列表。根据第二个pid2，找到pid1和pid2的相似度
    val simProductsMatrix = spark
      .read
      .option("uri",config("mongo.uri"))
      .option("collection",MONGODB_PRODUCT_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()   //DataFrame形式，如果以这个形式做后续的相似度查询，需要加查询条件等
      .as[ProductRecs]
      .rdd  //为了后续查询相似度方便，根据kv形式得到商品的相似度列表，转换成map结构
      .map{item =>
        (item.productId,item.recs.map(x=> (x.productId,x.score)).toMap)
      }.collectAsMap()
    val simProductsMatrixBroadCast = sc.broadcast(simProductsMatrix)

    //创建到Kafka的连接
    val kafkaPara = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    //创建一个DStream
    val kafkaStream =
      KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String,String](Array(config("kafka.topic")),kafkaPara))

    // UID|MID|SCORE|TIMESTAMP
    // 对kafkaDSStream进行处理，产生评分流
    //数据格式： userId|productId|score|timestamp  想要的数据格式
    val ratingStream = kafkaStream.map { msg =>
      var attr = msg.value().split("\\|")
      (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }


    // 核心实时推荐算法
    // 核心算法部分，定义评分流的处理流程
    ratingStream.foreachRDD{
      rdds => rdds.foreach{
        case ( userId, productId, score, timestamp ) =>
          println("rating data coming!>>>>>>>>>>>>>>>>>>")

          // TODO: 核心算法流程
          // 1. 从redis里取出当前用户的最近评分，保存成一个数组Array[(productId, score)]
          val userRecentlyRatings = getUserRecentlyRatings( MAX_USER_RATINGS_NUM, userId, ConnHelper.jedis )

          // 2. 从相似度矩阵中获取当前商品最相似的商品列表，作为备选列表，保存成一个数组Array[productId]
          val candidateProducts = getTopSimProducts( MAX_SIM_PRODUCTS_NUM, productId, userId, simProductsMatrixBroadCast.value )

          // 3. 计算每个备选商品的推荐优先级，得到当前用户的实时推荐列表，保存成 Array[(productId, score)]
          val streamRecs = computeProductScore( candidateProducts, userRecentlyRatings, simProductsMatrixBroadCast.value )

          // 4. 把推荐列表保存到mongodb
          saveDataToMongoDB( userId, streamRecs )
      }
    }
    // 启动streaming
    ssc.start()
    println("streaming started!")
    ssc.awaitTermination()
  }


  /**
   * 从redis里获取最近num次评分
   */
  import scala.collection.JavaConversions._
  def getUserRecentlyRatings(num: Int, userId: Int, jedis: Jedis): Array[(Int, Double)] = {
    // 从redis中用户的评分队列里获取评分数据，list键名为uid:USERID，值格式是 PRODUCTID:SCORE
    jedis.lrange( "userId:" + userId.toString, 0, num )
      .map{ item =>
        val attr = item.split("\\:")
        ( attr(0).trim.toInt, attr(1).trim.toDouble )  //productId, score
      }
      .toArray
  }

  // 获取当前商品的相似列表，并过滤掉用户已经评分过的，作为备选列表  simProducts:相似度矩阵
  def getTopSimProducts(num: Int,
                        productId: Int,
                        userId: Int,
                        simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                       (implicit mongoConfig: MongoConfig): Array[Int] ={
    // 从广播变量相似度矩阵中拿到当前商品的相似度列表
    val allSimProducts = simProducts(productId).toArray

    // 获得用户已经评分过的商品，过滤掉，排序输出
    val ratingCollection = ConnHelper.mongoClient( mongoConfig.db )( MONGODB_RATING_COLLECTION )
    val ratingExist = ratingCollection.find( MongoDBObject("userId"->userId) )
      .toArray
      .map{item=> // 只需要productId
        item.get("productId").toString.toInt
      }
    // 从所有的相似商品中进行过滤，过滤掉用户已经评分过的
    allSimProducts.filter( x => ! ratingExist.contains(x._1) )
      .sortWith(_._2 > _._2)
      .take(num)
      .map(x=>x._1)
  }

  // 计算每个备选商品的推荐得分
  //备选商品列表candidateProducts
  //已评分商品userRecentlyRatings
  def computeProductScore(candidateProducts: Array[Int],
                          userRecentlyRatings: Array[(Int, Double)],
                          simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
  : Array[(Int, Double)] ={
    // 定义一个长度可变数组ArrayBuffer，用于保存每一个备选商品的基础得分，(productId, score)
    val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    // 定义两个map，用于保存每个商品的高分和低分的计数器，productId -> count
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    // 遍历每个备选商品，计算和已评分商品的相似度  每个productId，都得到一个评分（可能有重复的ProductId）
    for( candidateProduct <- candidateProducts; userRecentlyRating <- userRecentlyRatings ){
      // 从相似度矩阵中获取当前备选商品和当前已评分商品间的相似度
      val simScore = getProductsSimScore( candidateProduct, userRecentlyRating._1, simProducts )
      if( simScore > 0.4 ){
        // 按照公式进行加权计算，得到基础评分
        scores += ( (candidateProduct, simScore * userRecentlyRating._2) )
        if( userRecentlyRating._2 > 3 ){
          increMap(candidateProduct) = increMap.getOrDefault(candidateProduct, 0) + 1
        } else {
          decreMap(candidateProduct) = decreMap.getOrDefault(candidateProduct, 0) + 1
        }
      }
    }

    // 根据公式计算所有的推荐优先级，首先以productId做groupby  一个productId，对应一个得分列表（因为已经做了groupBy聚合了）
    scores.groupBy(_._1).map{
      case (productId, scoreList) =>
        ( productId, scoreList.map(_._2).sum/scoreList.length + log(increMap.getOrDefault(productId, 1)) - log(decreMap.getOrDefault(productId, 1)) )
    }
      // 返回推荐列表，按照得分排序
      .toArray
      .sortWith(_._2>_._2)   //第二个元素是得分，第一个元素是productId
  }

  def getProductsSimScore(product1: Int, product2: Int,
                          simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Double ={
    simProducts.get(product1) match {   //Some(sims)表示product1有值，返回其相似度列表
      case Some(sims) => sims.get(product2) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }
  // 自定义log函数，以N为底
  def log(m: Int): Double = {
    val N = 10
    math.log(m)/math.log(N)
  }
  // 写入mongodb
  def saveDataToMongoDB(userId: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit ={
    val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(STREAM_RECS)
    // 按照userId查询并更新
    streamRecsCollection.findAndRemove( MongoDBObject( "userId" -> userId ) )
    streamRecsCollection.insert( MongoDBObject( "userId" -> userId,
      "recs" -> streamRecs.map(x=>MongoDBObject("productId"->x._1, "score"->x._2)) ) )
  }
}
