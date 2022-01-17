package edu.neu.csye7200.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import kafka.Kafka
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis



// Define the connection helper object, serialize
object ConnHelper extends Serializable{
  lazy val jedis = new Jedis("localhost")
  lazy val mongoClient = MongoClient( MongoClientURI("mongodb://localhost:27017/recommender") )
}

case class MongoConfig(uri:String, db:String)

// Define a benchmark recommendation object
case class Recommendation( mid: Int, score: Double )

// Define a user recommendation list based on the predicted score
case class UserRecs( uid: Int, recs: Seq[Recommendation] )

// Define movie similarity list based on LFM movie feature vector
case class MovieRecs( mid: Int, recs: Seq[Recommendation] )

object StreamingRecommender {

  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StreamingRecommender")

    // create a SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // get streaming context
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))    // batch duration

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // Load movie similarity matrix data and broadcast it
    val simMovieMatrix = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map{ movieRecs =>
        (movieRecs.mid, movieRecs.recs.map( x=> (x.mid, x.score) ).toMap )
      }.collectAsMap()

    val simMovieMatrixBroadCast = sc.broadcast(simMovieMatrix)

    // Define Kafka connection parameters
    val kafkaParam = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )
    // Create a DStream through kafka
    val kafkaStream = KafkaUtils.createDirectStream[String, String]( ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String]( Array(config("kafka.topic")), kafkaParam )
    )

    // Convert raw data UID|MID|SCORE|TIMESTAMP into a score stream
    val ratingStream = kafkaStream.map{
      msg =>
        val attr = msg.value().split("\\|")
        ( attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt )
    }

    // Continue to do streaming, the core real-time algorithm part
    ratingStream.foreachRDD{
      rdds => rdds.foreach{
        case (uid, mid, score, timestamp) => {
          println("rating data coming! >>>>>>>>>>>>>>>>")

          // 1. Get the latest K scores of the current user from redis
          // save them as Array[(mid, score)]
          val userRecentlyRatings = getUserRecentlyRating( MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis )

          // 2. Take out the most similar N movies of the current movie
          // from the similarity matrix as a candidate list，Array[mid]
          val candidateMovies = getTopSimMovies( MAX_SIM_MOVIES_NUM, mid, uid, simMovieMatrixBroadCast.value )

          // 3. For each candidate movie, calculate the recommendation priority
          // to get the current user's real-time recommendation list，Array[(mid, score)]
          val streamRecs = computeMovieScores( candidateMovies, userRecentlyRatings, simMovieMatrixBroadCast.value )

          // 4. Save recommended data to mongodb
          saveDataToMongoDB( uid, streamRecs )
        }
      }
    }
    // Start receiving and processing data
    ssc.start()

    println(">>>>>>>>>>>>>>> streaming started!")

    ssc.awaitTermination()

  }

  // The redis operation returns a java class.
  // In order to use the map operation, you need to introduce a conversion class
  import scala.collection.JavaConversions._

  def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {
    // Read data from redis
    // user score data is stored in the queue with uid:UID as the key, and value is MID:SCORE
    jedis.lrange("uid:" + uid, 0, num-1)
      .map{
        item =>
          val attr = item.split("\\:")
          ( attr(0).trim.toInt, attr(1).trim.toDouble )
      }
      .toArray
  }

  /**
   * Get num movies similar to the current movie as a candidate movie
   * @param num       Number of similar movies
   * @param mid        ID of current movie
   * @param uid       Current rating user ID
   * @param simMovies Similarity matrix
   * @return          Filtered candidate movie list
   */
  def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                     (implicit mongoConfig: MongoConfig): Array[Int] ={
    // 1. Get all similar movies from the similarity matrix
    val allSimMovies = simMovies(mid).toArray

    // 2. Query the movies that the user has watched from mongodb
    val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
      .find( MongoDBObject("uid" -> uid) )
      .toArray
      .map{
        item => item.get("mid").toString.toInt
      }

    // 3. Filter what you have seen to get the output list
    allSimMovies.filter( x=> ! ratingExist.contains(x._1) )
      .sortWith(_._2>_._2)
      .take(num)
      .map(x=>x._1)
  }

  def computeMovieScores(candidateMovies: Array[Int],
                         userRecentlyRatings: Array[(Int, Double)],
                         simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Array[(Int, Double)] ={
    // Define an ArrayBuffer to store the basic score of each candidate movie
    val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    // Define a HashMap to save the enhancement and reduction factors of each candidate movie
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    for( candidateMovie <- candidateMovies; userRecentlyRating <- userRecentlyRatings){
      // Get the similarity between the candidate movie and the recently rated movie
      val simScore = getMoviesSimScore( candidateMovie, userRecentlyRating._1, simMovies )

      if(simScore > 0.7){
        // Calculate the basic recommendation score of candidate movies
        scores += ( (candidateMovie, simScore * userRecentlyRating._2) )
        if( userRecentlyRating._2 > 3 ){
          increMap(candidateMovie) = increMap.getOrDefault(candidateMovie, 0) + 1
        } else{
          decreMap(candidateMovie) = decreMap.getOrDefault(candidateMovie, 0) + 1
        }
      }
    }
    // Do groupby according to the mid of the candidate movie
    // and find the final recommendation score according to the formula
    scores.groupBy(_._1).map{
      // groupBy,Data obtained afterwards, Map( mid -> ArrayBuffer[(mid, score)] )
      case (mid, scoreList) =>
        ( mid, scoreList.map(_._2).sum / scoreList.length + log(increMap.getOrDefault(mid, 1)) - log(decreMap.getOrDefault(mid, 1)) )
    }.toArray.sortWith(_._2>_._2)
  }

  // Get the similarity between two movies
  def getMoviesSimScore(mid1: Int, mid2: Int, simMovies: scala.collection.Map[Int,
    scala.collection.immutable.Map[Int, Double]]): Double ={

    simMovies.get(mid1) match {
      case Some(sims) => sims.get(mid2) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  // To find the logarithm of a number, use the base-changing formula, the base is 10 by default
  def log(m: Int): Double ={
    val N = 10
    math.log(m)/ math.log(N)
  }

  def saveDataToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit ={
    // Define the connection to the StreamRecs table
    val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

    // If there is data corresponding to uid in the table, delete it
    streamRecsCollection.findAndRemove( MongoDBObject("uid" -> uid) )
    // Store streamRecs data in the table
    streamRecsCollection.insert( MongoDBObject( "uid"->uid,
      "recs"-> streamRecs.map(x=>MongoDBObject( "mid"->x._1, "score"->x._2 )) ) )
  }

}

