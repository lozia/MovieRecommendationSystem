package edu.neu.csye7200.offline

import breeze.numerics.sqrt
import edu.neu.csye7200.offline.OfflineRecommender.MONGODB_RATING_COLLECTION
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession



object ALSTrainer {
  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")

    // create a SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // Load rating data
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map( rating => Rating( rating.uid, rating.mid, rating.score ) )    // Convert to rdd, and remove the timestamp
      .cache()

    // Randomly split the data set, generate training set and test set
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
    val trainingRDD = splits(0)
    val testRDD = splits(1)

    // Model parameter selection, output optimal parameters
    adjustALSParam(trainingRDD, testRDD)

    spark.close()
  }

  def adjustALSParam(trainData: RDD[Rating], testData: RDD[Rating]): Unit ={
    val result = for( rank <- Array(50, 100, 200, 300); lambda <- Array( 0.01, 0.1, 1 ))
      yield {
        val model = ALS.train(trainData, rank, 5, lambda)
        // Calculate the rmse of the model corresponding to the current parameter
        // and return Double
        val rmse = getRMSE( model, testData )
        ( rank, lambda, rmse )
      }
    // The console prints out the optimal parameters
    println(result.minBy(_._3))
  }

  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    // Calculate prediction score
    val userProducts = data.map(item => (item.user, item.product))
    val predictRating = model.predict(userProducts)

    // Using uid and mid as foreign keys
    // inner join actual observations and predicted values
    val observed = data.map( item => ( (item.user, item.product), item.rating ) )
    val predict = predictRating.map( item => ( (item.user, item.product), item.rating ) )
    // inner join ,get(uid, mid),(actual, predict)
    sqrt(
      observed.join(predict).map{
        case ( (uid, mid), (actual, pre) ) =>
          val err = actual - pre
          err * err
      }.mean()
    )
  }
}
