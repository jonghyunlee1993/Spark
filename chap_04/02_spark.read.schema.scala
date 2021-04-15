// :paste
// ---------------------------------------------------------------------------------------
import org.apache.spark.sql.types.{StructField, StructType, DateType, LongType, StringType}
import org.apache.spark.sql.types.Metadata

val schemaAiRoBiC =
     StructType(Array(
          StructField("bicNumber", StringType, true),
          StructField("rentDate", DateType, true),
          StructField("rentStatId", StringType, true),
          StructField("rentStatName", StringType, true),
          StructField("rentParkId", StringType, true),
          StructField("returnDate", DateType, true),
          StructField("returnStatId", StringType, true),
          StructField("returnStatName", StringType, true),
          StructField("returnParkId", StringType, true),
          StructField("useTime", LongType, true),
          StructField("useDistance", LongType, true)))

val df = spark.read
              .format("csv")
              .schema(schemaAiRoBiC)
              .load("AiRoBiC_2015_0519_sample.csv")
//            .load("C:/DevTools/workspaces/Spark-TDG/chap_04/AiRoBiC_2015_0519_sample.csv")

// Ctrl+D
// ---------------------------------------------------------------------------------------
df.printSchema
df.show
