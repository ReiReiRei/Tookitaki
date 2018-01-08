package reireiei.tookitaki

import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Main {
  def main(args: Array[String]): Unit = {

    val cmdConfig = KBoxConfigCmd.parser.parse(args, KBoxConfigCmd()) match {
      case Some(x) => x
      case None    => throw new RuntimeException("Invalid comand line args")
    }
    import Config._
    import cmdConfig._
    val spark =
      SparkSession.builder().appName(appName).getOrCreate()
    import spark.implicits._

    val schema = StructType(
      StructField("msno", StringType, false) :: StructField(
        "payment_method_id",
        IntegerType) :: StructField("payment_plan_days", IntegerType) :: StructField(
        "plan_list_price",
        IntegerType) :: StructField("actual_amount_paid", IntegerType) :: StructField(
        "is_auto_renew",
        IntegerType) :: StructField("transaction_date", StringType) :: StructField(
        "membership_expire_date",
        StringType) :: StructField("is_cancel", IntegerType) :: Nil
    )

    def timestampBasedWindow(cols: Seq[String],
                             timeStampCol: String,
                             start: Double,
                             end: Double): WindowSpec = {
      val secondsInYear = 60 * 60 * 24 * 365
      val effStart = (start * secondsInYear).toLong
      val effEnd = (end * secondsInYear).toLong
      //note it's not correct that there is 365 days in the year , but we need this this simplification for effisient

      Window
        .partitionBy(cols.head, cols.tail: _*)
        .orderBy(timeStampCol)
        .rangeBetween(effStart, effEnd)
    }
    def lastYearWindow(cols: Seq[String]) =
      timestampBasedWindow(cols, "transaction_timestamp", -1.0, 0)

    def msnoLastYear = lastYearWindow(Seq("msno"))
    spark.read
      .option("header", true)
      .schema(schema)
      .csv(inputDir)
      .repartition(col("msno")) // all operation are msno based
      .withColumn("transaction_date",
                  to_timestamp($"transaction_date", "yyyyMMdd"))
      .withColumn("membership_expire_date",
                  to_timestamp($"membership_expire_date", "yyyyMMdd"))
      .withColumn("transaction_timestamp",
                  col("transaction_date").cast(LongType))
      //1
      .withColumn("freq_count_payment_methods_previous_year",
                  count("payment_method_id").over(
                    lastYearWindow(Seq("msno", "payment_method_id"))
                  ))
      .withColumn(
        "freq_count_plan_days_previous_year",
        count("payment_plan_days").over(
          lastYearWindow(Seq("msno", "payment_plan_days")))
      )
      //2
      .withColumn(
        "max_plan_list_price_previous_year",
        max("plan_list_price").over(msnoLastYear)
      )
      .withColumn(
        "mean_plan_list_price_previous_year",
        mean("plan_list_price").over(msnoLastYear)
      )
      //3
      .withColumn("max_actual_amount_paid_previous_year",
                  max("actual_amount_paid").over(msnoLastYear))
      .withColumn("mean_actual_amount_paid_previous_year",
                  mean("actual_amount_paid").over(msnoLastYear))
      //4
      .withColumn(
        "decreasing_mean_actual_amount_paid_6_month",
        col("mean_actual_amount_paid_previous_year") / mean(
          "actual_amount_paid").over(
          timestampBasedWindow(Seq("msno"), "transaction_timestamp", -0.5, 0))
      )
      //5
      .withColumn("actual_amount_paid_auto_renewed",
                  col("actual_amount_paid") * col("is_auto_renew"))
      .withColumn("max_actual_amount_paid_renewed",
                  max("actual_amount_paid_auto_renewed").over(msnoLastYear))
      .drop("actual_amount_paid_auto_renewed")
      //6
      .withColumn("total_canceled_div_by_transaction_number_previous_year",
                  sum("is_cancel").over(msnoLastYear) / count("is_cancel").over(
                    msnoLastYear))
      .drop("transaction_timestamp")
      .coalesce(20)
      .write
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .csv(outputDir)

  }
}
