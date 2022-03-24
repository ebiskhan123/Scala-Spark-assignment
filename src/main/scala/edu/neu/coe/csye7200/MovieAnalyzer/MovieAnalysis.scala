package edu.neu.coe.csye7200.MovieAnalyzer

import com.phasmidsoftware.table.Table
import edu.neu.coe.csye7200.MovieAnalyzer.MovieAnalysis.df
import org.apache.log4j.lf5.LogLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}



object MovieAnalysis extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("MovieAnalysis")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  val path = "/Users/ebby/Documents/Grad/Scala/src/main/resources/movie_metadata.csv";
  val df = spark.read.option("header", "true").csv(path)
  df.show(3)
  ProcessData.findMean(df,"imdb_score")show()
  ProcessData.findStDev(df,"imdb_score")show()

}

object ProcessData {
  def findMean( df:DataFrame,  col: String):DataFrame = {
    df.select(avg(col))
  }

  def findStDev( df:DataFrame,  col: String):DataFrame = {
    df.select(stddev_pop(col))
  }
}

