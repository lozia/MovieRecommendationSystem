package edu.neu.csye7200.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int )

case class MongoConfig(uri:String, db:String)

// Define a benchmark recommendation object
case class Recommendation( mid: Int, score: Double )

// Define the top 10 recommended objects for movie categories
case class GenresRecommendation( genres: String, recs: Seq[Recommendation] )

object StatisticsRecommender {

  // Define table name
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  //The name of the statistical table
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // create a sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommeder")

    // create a SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // load data from mongodb
    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    // Create a temporary table named ratings
    ratingDF.createOrReplaceTempView("ratings")

    // ToDO:define different statistical recommendation results

    // 1. Historical popular statistics, with the most historical scoring data，mid，count
    val rateMoreMoviesDF = spark.sql("select mid, count(mid) as count from ratings group by mid")
    // Write the result to the corresponding mongodb table
    storeDFInMongoDB( rateMoreMoviesDF, RATE_MORE_MOVIES )

    // 2. Recent popular statistics, select the most recent rating data according to the "yyyyMM" format, and count the number of ratings
    // Create a date formatting tool
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    // Register udf, convert the timestamp into year and month format
    spark.udf.register("changeDate", (x: Int)=>simpleDateFormat.format(new Date(x * 1000L)).toInt )

    // Preprocess the original data, remove the uid
    val ratingOfYearMonth = spark.sql("select mid, score, changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

    // Find the rating of the movie in each month from the ratingOfMonth，mid，count，yearmonth
    val rateMoreRecentlyMoviesDF = spark.sql("select mid, count(mid) as count, yearmonth from ratingOfMonth group by yearmonth, mid order by yearmonth desc, count desc")

    // save into mongodb
    storeDFInMongoDB(rateMoreRecentlyMoviesDF, RATE_MORE_RECENTLY_MOVIES)

    // 3. High-quality movie statistics, statistics on average movie ratings，mid，avg
    val averageMoviesDF = spark.sql("select mid, avg(score) as avg from ratings group by mid")
    storeDFInMongoDB(averageMoviesDF, AVERAGE_MOVIES)

    // 4. Top statistics of movies in each category
    // Define all categories
    val genres = List("Action","Adventure","Animation","Comedy","Crime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery"
      ,"Romance","Science","Tv","Thriller","War","Western")

    // Add the average rating to the movie table and add a column，inner join
    val movieWithScore = movieDF.join(averageMoviesDF, "mid")

    // To make a Cartesian product, convert genres to rdd
    val genresRDD = spark.sparkContext.makeRDD(genres)

    // Calculate the category top10, first do the Cartesian product of the category and the movie
    val genresTopMoviesDF = genresRDD.cartesian(movieWithScore.rdd)
      .filter{
        // Condition filtering to find out the genres value of the movie field (Action|Adventure|Sci-Fi)Contains those of the current category   genre (Action)
        case (genre, movieRow) => movieRow.getAs[String]("genres").toLowerCase.contains( genre.toLowerCase )
      }
      .map{
        case (genre, movieRow) => ( genre, ( movieRow.getAs[Int]("mid"), movieRow.getAs[Double]("avg") ) )
      }
      .groupByKey()
      .map{
        case (genre, items) => GenresRecommendation( genre, items.toList.sortWith(_._2>_._2).take(10).map( item=> Recommendation(item._1, item._2)) )
      }
      .toDF()

    storeDFInMongoDB(genresTopMoviesDF, GENRES_TOP_MOVIES)

    spark.stop()
  }

  def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit ={
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

}

