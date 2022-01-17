package edu.neu.csye7200.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix



// LFM based on rating data, only rating data is needed
case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Int )

case class MongoConfig(uri:String, db:String)

// Define a benchmark recommendation object
case class Recommendation( mid: Int, score: Double )

// Define a user recommendation list based on the predicted score
case class UserRecs( uid: Int, recs: Seq[Recommendation] )

// Define movie similarity list based on LFM movie feature vector
case class MovieRecs( mid: Int, recs: Seq[Recommendation] )

object OfflineRecommender {

  // Define table names and constants
  val MONGODB_RATING_COLLECTION = "Rating"

  val USER_RECS = "UserRecs"
  val MOVIE_RECS = "MovieRecs"

  val USER_MAX_RECOMMENDATION = 20

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


    // load data
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map( rating => ( rating.uid, rating.mid, rating.score ) )    // Convert to rdd, and remove the timestamp
      .cache()

    // Extract all the uid and mid from the rating data and remove the duplicates
    val userRDD = ratingRDD.map(_._1).distinct()
    val movieRDD = ratingRDD.map(_._2).distinct()

    // Training latent semantic model
    val trainData = ratingRDD.map( x => Rating(x._1, x._2, x._3) )

    val (rank, iterations, lambda) = (200, 5, 0.1)
    val model = ALS.train(trainData, rank, iterations, lambda)

    // Calculate the predicted score based on the hidden features of the user and the movie, and get the user's recommendation list
    // Calculate the Cartesian product of user and movie to get an empty score matrix
    val userMovies = userRDD.cartesian(movieRDD)

    // Call model's predict method to predict the score
    val preRatings = model.predict(userMovies)

    val userRecs = preRatings
      .filter(_.rating > 0)    // Filter out items with a score greater than 0
      .map(rating => ( rating.user, (rating.product, rating.rating) ) )
      .groupByKey()
      .map{
        case (uid, recs) => UserRecs( uid, recs.toList.sortWith(_._2>_._2).take(USER_MAX_RECOMMENDATION).map(x=>Recommendation(x._1, x._2)) )
      }
      .toDF()

    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // Calculate the similarity matrix based on the hidden features of the movie
    // to get the similarity list of the movie
    val movieFeatures = model.productFeatures.map{
      case (mid, features) => (mid, new DoubleMatrix(features))
    }

    // Calculate the similarity of all movies pairwise
    // do the Cartesian product first
    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter{
        // Filter out the pairing between yourself and yourself
        case (a, b) => a._1 != b._1
      }
      .map{
        case (a, b) => {
          val simScore = this.consinSim(a._2, b._2)
          ( a._1, ( b._1, simScore ) )
        }
      }
      .filter(_._2._2 > 0.6)    // Filter out those with a similarity greater than 0.6
      .groupByKey()
      .map{
        case (mid, items) => MovieRecs( mid, items.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)) )
      }
      .toDF()
    movieRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }

  // Find the vector cosine similarity
  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix):Double ={
    movie1.dot(movie2) / ( movie1.norm2() * movie2.norm2() )
  }

}
