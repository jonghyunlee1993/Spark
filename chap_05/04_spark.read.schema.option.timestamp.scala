// :paste --------------------------------------------------------------------------------
import org.apache.spark.sql.types.{StructField, StructType, TimestampType, LongType, StringType}
import org.apache.spark.sql.types.Metadata

val schemaAiRoBiC =
     StructType(Array(
          StructField("bicNumber", StringType, true),
          StructField("rentDate", TimestampType, true),
          StructField("rentStatId", StringType, true),
          StructField("rentStatName", StringType, true),
          StructField("rentParkId", StringType, true),
          StructField("returnDate", TimestampType, true),
          StructField("returnStatId", StringType, true),
          StructField("returnStatName", StringType, true),
          StructField("returnParkId", StringType, true),
          StructField("useTime", LongType, true),
          StructField("useDistance", LongType, true)))

val df = spark.read
              .option("header", true)
              .option("timestampFormat","yyyy-MM-dd HH:mm")
              .format("csv")
              .schema(schemaAiRoBiC)
              .load("AiRoBiC_2015_0919_null.csv")

// Ctrl+D
// ---------------------------------------------------------------------------------------
df.printSchema
df.show
