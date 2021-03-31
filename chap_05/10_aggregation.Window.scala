import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{StructField, StructType,
                                   DateType, LongType, StringType}

// -- schema for CSV file --------------------
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

// -- 107,858 rows ---------------------------
val df = spark.read
    .option("header", true)
    .option("timestampFormat","yyyy-MM-dd HH:mm")
    .format("csv")
    .schema(schemaAiRoBiC)
    .load("AiRoBiC_2015_0919_ALL.csv")
//    .load("C:/DevTools/workspaces/Spark-TDG/chap_05/AiRoBiC_2015_0919_ALL.csv")

// -- groupBy --------------------------------
val grpDf = df.groupBy("rentStatId", "rentDate")
    .agg(sum("useTime").alias("useTimePerDay"),
         sum("useDistance").alias("useDistPerDay"))
    .select("rentStatId", "rentDate", "useTimePerDay", "useDistPerDay")

// -- Window Specification -------------------
val winSpec = Window
    .partitionBy("rentStatId")
    .orderBy("rentStatId", "rentDate")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

// -- Window Aggregation ---------------------
val winDf = grpDf
    .withColumn("timeCumulSum", sum($"useTimePerDay").over(winSpec))
    .select("rentStatId", "rentDate", "useTimePerDay", "timeCumulSum")
    .sort("rentStatId", "rentDate")

// -- Action ---------------------------------
winDf.show(10)
