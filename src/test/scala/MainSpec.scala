import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class MainSpec extends AnyFunSuite {
  private val spark: SparkSession = SparkSession.builder()
    .appName("ProviderRosterTest")
    .master("local[*]")
    .getOrCreate()

  test("readCSV should read data from CSV file") {
    val path = "src/test/test-data/providers_test.csv"
    val providersDF = Main.readCSV(spark, path, header = true, "|")
    assert(providersDF.isInstanceOf[DataFrame])
    assert(providersDF.count() > 0)
  }

  test("readCSV should throw exception when CSV file path does not exist") {
    val nonExistingPath = "non_existing_file.csv"

    val exception = intercept[Exception] {
      Main.readCSV(spark, nonExistingPath, header = true, "|")
    }
    assert(exception.isInstanceOf[org.apache.spark.sql.AnalysisException])
  }

  test("calculateTotalVisitsPerProvider should calculate correct total visits per provider") {
    val providersData = Seq(
      ("1", "John", "h", "Doe", "Cardiology"),
      ("2", "Jane", "h", "Smith", "Pediatrics")
    )

    val visitsData = Seq(
      ("1", "1", "2023-01-01"),
      ("2", "1", "2023-02-01"),
      ("3", "2", "2023-01-15")
    )
    val providersDF = spark.createDataFrame(providersData).toDF("provider_id", "first_name", "middle_name", "last_name", "provider_specialty")
    val visitsDF = spark.createDataFrame(visitsData).toDF("visit_id", "provider_id", "date_of_service")

    val resultDF = Main.calculateTotalVisitsPerProvider(providersDF, visitsDF)
    val expectedData = Seq(
      ("1", "John", "h", "Doe", "Cardiology", 2),
      ("2", "Jane", "h", "Smith", "Pediatrics", 1)
    )

    val expectedDF = spark.createDataFrame(expectedData).toDF("provider_id", "first_name", "middle_name", "last_name", "provider_specialty", "total_visits")
    assertDataFrameEquals(resultDF, expectedDF)
  }

  test("calculateTotalVisitsPerProviderPerMonth should calculate correct total visits per provider per month") {
    val visitsData = Seq(
      ("1", "1", "2023-01-01"),
      ("2", "1", "2023-02-01"),
      ("3", "2", "2023-01-15")
    )
    val visitsDF = spark.createDataFrame(visitsData).toDF("visit_id", "provider_id", "date_of_service")
    val resultDF = Main.calculateTotalVisitsPerProviderPerMonth(visitsDF)
    val expectedData = Seq(
      ("2", "2023-01", 1),
      ("1", "2023-02", 1),
      ("1", "2023-01", 1)
    )
    val expectedDF = spark.createDataFrame(expectedData).toDF("provider_id", "month", "total_visits")
    assertDataFrameEquals(resultDF, expectedDF)

  }

  private def assertDataFrameEquals(actualDF: DataFrame, expectedDF: DataFrame): Unit = {
    assert(actualDF.collect().sameElements(expectedDF.collect()))
  }

  private def assertSchemaEquals(actualSchema: StructType, expectedSchema: StructType): Unit = {
    assert(actualSchema.length == expectedSchema.length)
    assert(actualSchema.map(_.name).toSet == expectedSchema.map(_.name).toSet)
  }

  def afterAll(): Unit = {
    spark.stop()
  }
}