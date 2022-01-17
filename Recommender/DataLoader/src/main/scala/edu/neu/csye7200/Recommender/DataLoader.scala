package edu.neu.csye7200.Recommender

import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient



/**
 * Movie data file format
 *
 * 260                                         Movie_id
 * Star Wars: Episode IV - A New Hope (1977)   Movie_name
 * Princess Leia is captured and held hostage  Detailed description
 * 121 minutes                                 length of the movie
 * September 21, 2004                          time of issue
 * 1977                                        time of shoot
 * English                                     type of language
 * Action|Adventure|Sci-Fi                     genres
 * Mark Hamill|Harrison Ford|Carrie Fisher     list of actors
 * George Lucas                                name of directors
 *
 */
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

/**
 * Rating.csv  dataset
 *
 * 1,31,2.5,1260759144
 */
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int )

/**
 * Tag.csv  dataset
 *
 * 15,1955,dentist,1193435061
 */
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)

// Encapsulate the configuration of mongo and es into sample classes

/**
 *
 * @param uri MongoDB connection
 * @param db  MongoDB database
 */
case class MongoConfig(uri:String, db:String)

/**
 *
 * @param httpHosts       http hosts, separated by commas
 * @param transportHosts  transport  hosts
 * @param index            The index to be manipulated
 * @param clustername      Cluster name, default: elasticsearch
 */
case class ESConfig(httpHosts:String, transportHosts:String, index:String, clustername:String)

object DataLoader {

  // Set constant values
  val MOVIE_DATA_PATH = "./Recommender/DataLoader/src/main/resources/movies.csv"
  val RATING_DATA_PATH = "./Recommender/DataLoader/src/main/resources/ratings.csv"
  val TAG_DATA_PATH = "./Recommender/DataLoader/src/main/resources/tags.csv"
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "localhost:9200",
      "es.transportHosts" -> "localhost:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "elasticsearch"
    )

    // create a sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")

    // create a SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    // load data
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)

    val movieDF = movieRDD.map(
      item => {
        val attr = item.split("\\^")
        Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim, attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
      }
    ).toDF()

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)

    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
    }).toDF()

    val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)
    //convert tagRDD to DataFrame
    val tagDF = tagRDD.map(item => {
      val attr = item.split(",")
      Tag(attr(0).toInt,attr(1).toInt,attr(2).trim,attr(3).toInt)
    }).toDF()

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // Save data to MongoDB
    storeDataInMongoDB(movieDF, ratingDF, tagDF)

    // Data preprocessing, add the tag information corresponding to the movie, add a column tag1|tag2|tag3...
    import org.apache.spark.sql.functions._

    /**
     * mid, tags
     *
     * tags: tag1|tag2|tag3...
     */
    val newTag = tagDF.groupBy($"mid")
      .agg( concat_ws( "|", collect_set($"tag") ).as("tags") )
      .select("mid", "tags")

    // newTag and movie do join, data are merged together, left outer join
    val movieWithTagsDF = movieDF.join(newTag, Seq("mid"), "left")

    implicit val esConfig = ESConfig(config("es.httpHosts"), config("es.transportHosts"), config("es.index"), config("es.cluster.name"))

    // save data to ES
    storeDataInES(movieWithTagsDF)

    spark.stop()
  }

  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit ={
    // create a new connection of mongodb
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    // If there is already a corresponding database in mongodb, delete it first
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    // Write DF data into the corresponding mongodb table
    movieDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //Index the data table
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    mongoClient.close()

  }

  def storeDataInES(movieDF: DataFrame)(implicit eSConfig: ESConfig): Unit ={
    // New es configuration
    val settings: Settings = Settings.builder().put("cluster.name", eSConfig.clustername).build()

    // Create a new es client
    val esClient = new PreBuiltTransportClient(settings)

    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    eSConfig.transportHosts.split(",").foreach{
      case REGEX_HOST_PORT(host: String, port: String) => {
        esClient.addTransportAddress(new InetSocketTransportAddress( InetAddress.getByName(host), port.toInt ))
      }
    }

    // Clean up the remaining data first
    if( esClient.admin().indices().exists( new IndicesExistsRequest(eSConfig.index) )
      .actionGet()
      .isExists
    ){
      esClient.admin().indices().delete( new DeleteIndexRequest(eSConfig.index) )
    }

    esClient.admin().indices().create( new CreateIndexRequest(eSConfig.index) )

    movieDF.write
      .option("es.nodes", eSConfig.httpHosts)
      .option("es.http.timeout", "100m")
      .option("es.mapping.id", "mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(eSConfig.index + "/" + ES_MOVIE_INDEX)
  }

}

