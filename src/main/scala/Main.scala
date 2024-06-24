import org.apache.spark.sql.functions.{col, count, date_format, to_date}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.Constants._

object Main {
  private val visitsSchema = StructType(Array(
    StructField("visit_id", StringType, nullable = false),
    StructField("provider_id", StringType, nullable = false),
    StructField("date_of_service", StringType, nullable = false)
  ))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ProviderRoster")
      .master("local[*]")
      .getOrCreate()

    val providersDF = readCSV(spark, providersPath, header = true, "|")
    val visitsDF = readCSV(spark, visitsPath, header = false, ",", Some(visitsSchema))

    // Problem 1: Calculate the total number of visits per provider partitioned by speciality
    val totalVisitsPerProviderDF = calculateTotalVisitsPerProvider(providersDF, visitsDF)
    totalVisitsPerProviderDF
      .coalesce(1)
      .write
      .partitionBy("provider_specialty")
      .json("output/problem1")


    // Problem 2: Calculate the total number of visits per provider per month
    val totalVisitsPerProviderPerMonthDF = calculateTotalVisitsPerProviderPerMonth(visitsDF)
    totalVisitsPerProviderPerMonthDF
      .coalesce(1)
      .write
      .json("output/problem2")

    spark.stop()
  }

  // CSV Reader Method
  def readCSV(spark: SparkSession, path: String, header: Boolean, delimiter: String, schema: Option[StructType] = None): DataFrame = {
    val reader = spark.read.option("header", header).option("delimiter", delimiter)
    schema match {
      case Some(s) => reader.schema(s).csv(path)
      case None => reader.csv(path)
    }
  }

  def calculateTotalVisitsPerProvider(providersDF: DataFrame, visitsDF: DataFrame): DataFrame = {
    visitsDF
      .groupBy("provider_id") //Group By provider id
      .agg(count("visit_id").as("total_visits")) //count total visits of provider in visitsDF
      .join(providersDF, visitsDF("provider_id") === providersDF("provider_id")) //Join table using provider_id and output results
      .select(
        providersDF("provider_id"),
        col("first_name"),
        col("middle_name"),
        col("last_name"),
        col("provider_specialty"),
        col("total_visits")
      )
  }

  def calculateTotalVisitsPerProviderPerMonth(visitsDF: DataFrame): DataFrame = {
    visitsDF
      .withColumn("month", date_format(to_date(col("date_of_service"), "yyyy-MM-dd"), "yyyy-MM")) //Fetch Column with month
      .groupBy("provider_id", "month") //Group by respective month
      .agg(count("visit_id").as("total_visits")) //Count visits according to those months
  }

}