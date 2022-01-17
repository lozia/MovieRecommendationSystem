package neu.edu.content

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix


// The required data source is movie content information
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

case class MongoConfig(uri:String, db:String)

// Define a benchmark recommendation object
case class Recommendation( mid: Int, score: Double )

// Define the movie similarity list of the feature vector extracted from the movie content information
case class MovieRecs( mid: Int, recs: Seq[Recommendation] )

object ContentRecommender {

  // Define table names and constants
  val MONGODB_MOVIE_COLLECTION = "Movie"

  val CONTENT_MOVIE_RECS = "ContentMovieRecs"

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

    // Load data and preprocess it
    val movieTagsDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .map(
        // Extract mid, name, and genres as the original content features
        // and the tokenizer will segment words according to spaces by default
        x => ( x.mid, x.name, x.genres.map(c=> if(c=='|') ' ' else c) )
      )
      .toDF("mid", "name", "genres")
      .cache()

    // Core part: Use TF-IDF to extract movie feature vectors from content information

    // Create a tokenizer, the default is to segment words by spaces
    val tokenizer = new Tokenizer().setInputCol("genres").setOutputCol("words")

    // Use the tokenizer to convert the original data to generate a new list of  words
    val wordsData = tokenizer.transform(movieTagsDF)

    // Introducing the HashingTF tool, you can convert a word sequence into a corresponding word frequency
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(50)
    val featurizedData = hashingTF.transform(wordsData)

    // Introduce the IDF tool, you can get the idf model
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    // Train the idf model to get the inverse document frequency of each word
    val idfModel = idf.fit(featurizedData)
    // Use the model to process the original data
    // to obtain the tf-idf of each word in the document as a new feature vector
    val rescaledData = idfModel.transform(featurizedData)

    //    rescaledData.show(truncate = false)

    val movieFeatures = rescaledData.map(
      row => ( row.getAs[Int]("mid"), row.getAs[SparseVector]("features").toArray )
    )
      .rdd
      .map(
        x => ( x._1, new DoubleMatrix(x._2) )
      )
    movieFeatures.collect().foreach(println)

    // Calculate the similarity of all movies pairwise, first do the Cartesian product
    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter{
        // Filter out the match between yourself and yourself
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
      .option("collection", CONTENT_MOVIE_RECS)
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
