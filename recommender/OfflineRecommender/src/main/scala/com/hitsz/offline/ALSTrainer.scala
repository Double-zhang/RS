package com.hitsz.offline
import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
object ALSTrainer {
  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    //创建SparkConf
    val sparkConf = new SparkConf().setAppName("ALSTrainer").setMaster(config("spark.cores"))
    //创建SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    import spark.implicits._

    //加载评分数据
    val ratingRDD = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",OfflineRecommender.MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd  //Rating类型的RDD   直接读的时候，就转换成RDD
      .map(rating => Rating(rating.userId,rating.productId,rating.score)).cache()

    // 将一个RDD随机切分成两个RDD，用以划分训练集和测试集
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))   //randomSplit返回的是一个RDD的数组：scala.Array[org.apache.spark.rdd.RDD[T]]

    //训练集
    val trainingRDD = splits(0)
    //测试集
    val testingRDD = splits(1)

    //输出最优参数
    adjustALSParams(trainingRDD, testingRDD)

    //关闭Spark
    spark.close()
  }
  // 输出最终的最优参数
  def adjustALSParams(trainData:RDD[Rating], testData:RDD[Rating]): Unit = {
    // 这里指定迭代次数为10，rank和lambda在几个值中选取调整
    val result = for (rank <- Array(100, 200, 250); lambda <- Array(1, 0.1, 0.01, 0.001))
      yield {  //把for循环里面的每一次的中间结果都保存下来
        val model = ALS.train(trainData, rank, 10, lambda)
        val rmse = getRMSE(model, testData)  //均方根误差
        (rank, lambda, rmse)
      }
    // 按照rmse排序
    println(result.minBy(_._3))  //最优参数输出到控制台
  }

  def getRMSE(model:MatrixFactorizationModel, data:RDD[Rating]):Double={
    //构建userProducts，得到预测评矩阵，将data本身的三元组，转换成一个二元组  。。。 item是Rating中的样例类。里面有userId，productId，score三个属性
    val userProducts = data.map(item => (item.user,item.product))
    val predictRating = model.predict(userProducts)  //返回：RDD[Rating]

    //首先把预测评分和实际评分表按照（userId,productId）做一个连接
    val real = data.map(item => ((item.user,item.product),item.rating)) //user对product这个商品的评分
    val predict = predictRating.map(item => ((item.user,item.product),item.rating))

    // 计算RMSE
    sqrt(  //sqrt做平方根
      real.join(predict).map{case ((userId,productId),(real,pre))=>
        // 真实值和预测值之间的差
        val err = real - pre
        err * err   //误差平方
      }.mean()  //求平均
    )
  }

}


