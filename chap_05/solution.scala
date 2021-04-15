// data loading with schema
import org.apache.spark.sql.types.{StructField, StructType, DateType, TimestampType, LongType, DoubleType, StringType}
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
          StructField("useDistance", DoubleType, true)))

val df = spark.read
              .option("header", true)
              .format("csv")
              .schema(schemaAiRoBiC)
              .load("data/seoul_bike.csv")

// 1. 1월 중 이용횟수가 가장 많은 날은 며칠이고, 그 횟수는?
df.groupBy("rentDate").agg(count("bicNumber").alias("Count")).orderBy($"Count".desc).show()


// 2. 1번의 해당 일자를 기준으로 이용빈도가 높은 대여소(rentStatId) Top 10을 대여소이름, 이용빈도와 함께 나열하라.
df.where("rentDate = '2021-01-25'").groupBy("rentStatID", "rentStatName").agg(count("bicNumber").alias("Count")).orderBy($"Count".desc).show(10)

// 3. 2번의 Top 10 대여소의 이용빈도가 높은 이유는 무엇이라고 생각하는지 자신의 생각을 써보자.
// 단, 어떤 추가적인 분석 없이 대여소이름을 보고 바로 떠오르는 생각을 적어보도록 하자.
// 이 날은 날씨가 좋아서 나들이가 많았을 것으로 추정된다. (실제로 이 날 날씨는 해당 일을 기준으로 일주일 안에 가장 포근한 날씨였다.)
// 여의나루, 뚝섬유원지, 롯데월드타워, 청계천은 대표적인 서울시의 유원지로 알려져 있다. 추가 분석이 필요하겠지만 다른 날짜에 비해서 해당 대여소에서 변화가 많이 발생했는지를 본다면 가설을 확고하게 검증할 수 있을 것으로 본다. 
// 현재는 시간에 대한 가정이 누락되어 있는데 이날은 월요일이었기 때문에 저녁 6시 이후, 이용 빈도가 치솟는 것을 확인할 수 있다면 가설을 지지할 것으로 생각한다. 