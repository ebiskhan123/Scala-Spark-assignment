package edu.neu.coe.csye7200.MovieAnalyzer

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class MovieAnalysisTest extends AnyFlatSpec with Matchers {

  behavior of "test of 20 rows"
  it should "test movie_metadata.csv" in {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("MovieAnalysis")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val path = "/Users/ebby/Documents/Grad/Scala/src/main/resources/movie_metadata.csv"
    val df = spark.read.option("header", "true").csv(path).limit(20)
    ProcessData.findMean(df,"imdb_score").first().getDouble(0) shouldBe(7.095000000000001)
    ProcessData.findStDev(df,"imdb_score").first().getDouble(0) shouldBe(0.6192535829528967)
  }


  behavior of "test the mean and std dev for the entire dataset"
  it should "test movie_metadata.csv" in {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("MovieAnalysis")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val path = "/Users/ebby/Documents/Grad/Scala/src/main/resources/movie_metadata.csv"
    val df = spark.read.option("header", "true").csv(path)
    ProcessData.findMean(df,"imdb_score").first().getDouble(0) shouldBe(6.453200745804848)
    ProcessData.findStDev(df,"imdb_score").first().getDouble(0) shouldBe(0.9984966998015917)
  }

}
