// -- get_json_object, json_tuple --------------------------------------------------------
import org.apache.spark.sql.functions.{get_json_object, json_tuple}

val json = """ '{"jsonKey" : {"jsonValue" : [1,2,3]}}' as jsonString """
val jsonDf = spark.range(1).selectExpr(json)

jsonDf.select(get_json_object(col("jsonString"), "$.jsonKey.jsonValue") as "jsonCol").show
jsonDf.select(get_json_object(col("jsonString"), "$.jsonKey.jsonValue[1]") as "jsonCol").show
jsonDf.select(json_tuple(col("jsonString"), "jsonKey")).show

// -- to_json, from_json -----------------------------------------------------------------
import org.apache.spark.sql.functions.{to_json, from_json}
import org.apache.spark.sql.types._

val jsonStruct = fillDf.selectExpr("(rentStatId, rentStatName) as jsonStruct")
jsonStruct.select(to_json(col("jsonStruct"))).show

val jsonMap = fillDf.select(map(col("rentStatId"), col("rentStatName")) as "jsonMap")
jsonMap.select(to_json(col("jsonMap"))).show

val jId = new StructField("rentStatId", StringType, true)
val jName = new StructField("rentStatName", StringType, true)
val jsonSchema = new StructType(Array(jId, jName))

jsonStruct.select(
              to_json(col("jsonStruct")).alias("jString")                
          ).select(from_json(col("jString"), jsonSchema)).show                     
                     