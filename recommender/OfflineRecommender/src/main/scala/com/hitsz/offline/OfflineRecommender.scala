package com.hitsz.offline

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

case class ProductRating(userId: Int, productId: Int, score: Double, timestamp: Int)

case class MongoConfig(uri:String, db:String)

// 标准推荐对象，productId,score
case class Recommendation(productId: Int, score:Double)

// 用户推荐列表
case class UserRecs(userId: Int, recs: Seq[Recommendation])

// 商品相似度（商品推荐）
case class ProductRecs(productId: Int, recs: Seq[Recommendation])

object OfflineRecommender {
  // 定义常量
  val MONGODB_RATING_COLLECTION = "Rating"

  // 推荐表的名称
  val USER_RECS = "UserRecs"
  val PRODUCT_RECS = "ProductRecs"
  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {
    // 定义配置
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    // 创建spark session
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    import spark.implicits._
    //读取mongoDB中的业务数据....这里使用的是RDD，因为最后调用ALS算法.train传入的数据要求是RDD
    val ratingRDD = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map(rating=> (rating.userId, rating.productId, rating.score)).cache()  //转换成ALS里面定义好的一个数据格式  .cache()持久化到内存，避免rdd重复计算

    //用户的数据集 RDD[Int]
    val userRDD = ratingRDD.map(_._1).distinct()
    val prodcutRDD = ratingRDD.map(_._2).distinct()

    /*
    1.训练隐语义模型
    2.获得预测评分矩阵，得到用户的推荐列表
    3.利用商品的特征向量，计算商品的相似度列表
    */

    //1.训练隐语义模型
    val trainData: RDD[Rating] = ratingRDD.map(x => Rating(x._1, x._2, x._3))

    //定义模型训练的参数，rank：隐特征个数，iterations：迭代次数，lambda：正则化系数
    val(rank,iterations,lambda) = (5,10,0.01)  //这里只是经验参数，不一定是最优参数！！
    val model = ALS.train(trainData, rank, iterations, lambda)

    //2.获得预测评分矩阵
    //用userRDD和productRDD做一个笛卡尔积，得到空的userProductRDD
    val userProducts = userRDD.cartesian(prodcutRDD)
    val preRating = model.predict(userProducts)
    //从预测评分矩阵中，得到用户推荐列表...
    val userRecs = preRating.filter(_.rating>0)
      .map(rating => (rating.user,(rating.product,rating.rating)))  //.map转换成就是要得到的用户和对应的推荐列表格式
      .groupByKey()
      .map{
      case(userId,recs) =>
        UserRecs(userId,recs.toList.sortWith(_._2>_._2)  //降序排列
          .take(USER_MAX_RECOMMENDATION)
          .map(x=>Recommendation(x._1,x._2)))  //.map是为了将得到的数据转换成要求的推荐列表要求的格式
    }
      .toDF()

    userRecs.write
      .option("uri",mongoConfig.uri)
      .option("collection",USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //计算商品的特征向量，得到商品的相似度列表
    //获取商品的特征矩阵，数据格式 RDD[(scala.Int, scala.Array[scala.Double])]
    val productFeatures = model.productFeatures.map{case (productId,features) =>
      (productId, new DoubleMatrix(features))  //做向量计算
    }

    // 计算笛卡尔积并过滤合并。。。两两配对商品，计算余弦相似度
    val productRecs = productFeatures.cartesian(productFeatures)
      .filter{case (a,b) => a._1 != b._1}     //不能自己和自己匹配
      .map{case (a,b) =>
        val simScore = this.consinSim(a._2,b._2) // 求余弦相似度  a的第二个元素是特征向量
        (a._1,(b._1,simScore))  //返回两个商品的余弦相似度
      }.filter(_._2._2 > 0.6)  //也就是simScore要大于0.6
      .groupByKey()
      .map{case (productId,items) =>
        ProductRecs(productId,items.toList.map(x => Recommendation(x._1,x._2)))
      }.toDF()

    productRecs
      .write
      .option("uri", mongoConfig.uri)
      .option("collection",PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }

  def consinSim(product1: DoubleMatrix, product2: DoubleMatrix): Double = {
    product1.dot(product2) / ( product1.norm2()  * product2.norm2() )  //dot表示点乘
  }
}
