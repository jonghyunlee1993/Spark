// *****************************************************************************
// 프로그램: 02_MovieRecommender.ALS.scala
// 작성검증: Barnabas Kim(likebnb@gmail.com)
// 수정일자: 2020-05-23
// 배포버전: 1.0.2
// -----------------------------------------------------------------------------
// 데이터셋: Movie Lens
// 분석주제: 문화.영화.평점
// 분석방법: 교차최소제곱(ALS)
// 컬럼특성: 사용자, 아이템(영화), 평점(레이블), 타임스탬프
// 소스제공: https://grouplens.org/datasets/movielens/
// *****************************************************************************
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, DoubleType, FloatType}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.mllib.evaluation.{RankingMetrics, RegressionMetrics}

// *************************************
//  PHASE-1 : 데이터준비
// *************************************
// SQL Table -> DataFrame
// -------------------------------------
val ratingDf = spark.sql("""
SELECT cast(userId as Integer) as userId,
       cast(movieId as Integer) as movieId,
       cast(rating as Double) as rating
  FROM ratings
""")
ratingDf.cache
ratingDf.count


// *************************************
//  PHASE-2 : 모델학습 및 예측(검증)
// *************************************
// 데이터분할(훈련셋 : 검증셋)
// -------------------------------------
val Array(trainSet, testSet) = ratingDf.randomSplit(Array(0.8, 0.2), 5043L)

// 모델 선언(교차최소제곱)
// -------------------------------------
val recommender = new ALS().
        setUserCol("userId").
        setItemCol("movieId").
        setRatingCol("rating").
        setMaxIter(3).
        setRegParam(0.01).
        setSeed(1234567)

// 파이프라인, 모델적합
// -------------------------------------
val pipeline = new Pipeline().setStages(Array(recommender))
val model = pipeline.fit(trainSet)
val predictions = model.transform(testSet)

// ****************************************************
// 튜닝팁: 메모리 문제발생 시 조치 방법
// java.lang.OutOfMemoryError: Java heap space
// spark-shell --driver-memory 4G
// ****************************************************

// 모델 검증
// -------------------------------------
val evaluator = new RegressionEvaluator().
        setLabelCol("rating").
        setPredictionCol("prediction").
        setMetricName("rmse")

val rmse = evaluator.evaluate(predictions)

// ****************************************************
// 평점이 없는 경우(ColdStart) -> "drop"
// scala> val rmse = evaluator.evaluate(predictions)
// rmse: Double = NaN
// ****************************************************

// Cold Start Strategy
// -------------------------------------
recommender.setColdStartStrategy("drop")

// 파이프라인 재설정, 적합 및 평가
// -------------------------------------
val pipeline = new Pipeline().setStages(Array(recommender))
val model = pipeline.fit(trainSet)
val predictions = model.transform(testSet)
val rmse = evaluator.evaluate(predictions)

val alsModel = model.stages(0).asInstanceOf[ALSModel]

// ----------------------------------------------------
// scala> val rmse = evaluator.evaluate(predictions)
// rmse: Double = 1.0679278387094164
// ----------------------------------------------------



// *************************************
//  PHASE-3 : 회귀평가지표
// *************************************
// 회귀평가지표 선언
// -------------------------------------
val rm = new RegressionMetrics(
                 predictions.select("rating", "prediction").
                 rdd.map(x => (x.getFloat(0), x.getFloat(1))))

rm.meanSquaredError
// ****************************************************
// Float 타입을 넘기려 시도했지만 암묵적 변환 실패
// scala> rm.meanSquaredError
// java.lang.ClassCastException: java.lang.Double cannot be cast to java.lang.Float
// ----------------------------------------------------

// rating, prediction 컬럼의 타입확인
// -------------------------------------
predictions.printSchema

// 명시적인 형변환: Double -> Float
// -------------------------------------
val rm = new RegressionMetrics(
                 predictions.select("rating", "prediction").
                 rdd.map(x => (x.getDouble(0).toFloat, x.getFloat(1))))

// 평균제곱오차: 이 값이 작을수록 좋다
// -------------------------------------
rm.meanSquaredError
rm.rootMeanSquaredError

// RegressionMetrics의 평가지표 확인
// -------------------------------------
println(s"""
---------------------------------------------------------------
1st Model Fitting & Validation
---------------------------------------------------------------
               MSE: ${rm.meanSquaredError}
               MAE: ${rm.meanAbsoluteError}
              RMSE: ${rm.rootMeanSquaredError}
         R Squared: ${rm.r2}
Explained Variance: ${rm.explainedVariance}
---------------------------------------------------------------
""")

// *************************************
//  PHASE-4 : 순위평가지표
// *************************************
// 사용자별 실제 평점(2.5 초과) 목록
// -------------------------------------
val perUserActual = predictions.
        where("rating > 2.5").
        groupBy("userId").
        agg(expr("collect_list(movieId) as movies"))

// 사용자별 평점 예측값 목록
// -------------------------------------
val perUserPredictions = predictions.
        orderBy($"userId", $"prediction".desc).
        groupBy($"userId").
        agg(expr("collect_list(movieId) as movies"))

// 사용자별 평점 실제값-예측값 목록
// -------------------------------------
val perUserActualvPred = perUserActual.join(perUserPredictions, Seq("userId")).
        map(row => (
           row(1).asInstanceOf[Seq[Integer]].toArray,
           row(2).asInstanceOf[Seq[Integer]].toArray.take(15)
        ))

// 순위평가지표 선언
// -------------------------------------
val ranks = new RankingMetrics(perUserActualvPred.rdd)

// 순위평가지표 확인
// -------------------------------------
ranks.meanAveragePrecision
ranks.precisionAt(5)
ranks.ndcgAt(5)


// *************************************
//  PHASE-5 : 평가지표 활용
// *************************************
// "Toy Story"에 평점을 준 사용자
// -------------------------------------
spark.sql("select * from movies where title like '%Toy Story%'").show
perUserActual.where(array_contains($"movies", 78499)).show(5)
+------+--------------------+
|userId|              movies|
+------+--------------------+
|   159|[1265, 539, 78499...|
|   103|[858, 48780, 1884...|
|   232|[36525, 1721, 693...|
|   292|[2096, 593, 71535...|
|   380|[2366, 36525, 858...|
+------+--------------------+
only showing top 5 rows

// 영화정보와 평점정보(사용자=159)를 조인
// ----------------------------------------------
val movieDf = spark.read.format("csv").option("header", true).load("movies.csv")
val userMovies = predictions.where("userId = 159").where("rating > 2.5").
      selectExpr("movieId as mId", "rating").sort(desc("rating"))

// 평점, 영화아이디, 제목, 장르
// ----------------------------------------------
userMovies.join(movieDf,
      movieDf("movieId") === userMovies("mId"), "inner").
      select("rating", "movieId", "title", "genres").show(false)

+------+-------+---------------------------+------------------------------------------------+
|rating|movieId|title                      |genres                                          |
+------+-------+---------------------------+------------------------------------------------+
|5.0   |78499  |Toy Story 3 (2010)         |Adventure|Animation|Children|Comedy|Fantasy|IMAX|
|5.0   |166643 |Hidden Figures (2016)      |Drama                                           |
|4.5   |1265   |Groundhog Day (1993)       |Comedy|Fantasy|Romance                          |
|4.5   |1      |Toy Story (1995)           |Adventure|Animation|Children|Comedy|Fantasy     |
|4.5   |157296 |Finding Dory (2016)        |Adventure|Animation|Comedy                      |
|4.0   |7143   |Last Samurai, The (2003)   |Action|Adventure|Drama|War                      |
|4.0   |89774  |Warrior (2011)             |Drama                                           |
|3.5   |89864  |50/50 (2011)               |Comedy|Drama                                    |
|3.0   |539    |Sleepless in Seattle (1993)|Comedy|Drama|Romance                            |
|3.0   |377    |Speed (1994)               |Action|Romance|Thriller                         |
|3.0   |5957   |Two Weeks Notice (2002)    |Comedy|Romance                                  |
|3.0   |110    |Braveheart (1995)          |Action|Drama|War                                |
|3.0   |84374  |No Strings Attached (2011) |Comedy|Romance                                  |
+------+-------+---------------------------+------------------------------------------------+


// 사용자, 159를 위한 영화 추천
// ----------------------------------------------
val user = perUserActual.where("userId = '159'")
val rcmdMovies = alsModel.recommendForUserSubset(user, 20).
      select(explode($"recommendations")).select($"col.movieId".alias("mId"), $"col.rating")
val joinDf = rcmdMovies.join(movieDf,
      movieDf("movieId") === rcmdMovies("mId"), "inner").
      select("rating", "movieId", "title", "genres")

joinDf.show(false)

+---------+-------+-----------------------------------------------------+--------------------------------------------------+
|rating   |movieId|title                                                |genres                                            |
+---------+-------+-----------------------------------------------------+--------------------------------------------------+
|9.378477 |45668  |Lake House, The (2006)                               |Drama|Fantasy|Romance                             |
|8.314578 |143969 |Er ist wieder da (2015)                              |Comedy                                            |
|7.8110566|3266   |Man Bites Dog (C'est arrivé près de chez vous) (1992)|Comedy|Crime|Drama|Thriller                       |
|7.7330904|142422 |The Night Before (2015)                              |Comedy                                            |
|7.6915383|6857   |Ninja Scroll (Jûbei ninpûchô) (1995)                 |Action|Adventure|Animation|Fantasy                |
|7.6465197|135887 |Minions (2015)                                       |Adventure|Animation|Children|Comedy               |
|7.6234965|932    |Affair to Remember, An (1957)                        |Drama|Romance                                     |
|7.619594 |938    |Gigi (1958)                                          |Musical                                           |
|7.4467325|100507 |21 and Over (2013)                                   |Comedy                                            |
|7.4386764|130520 |Home (2015)                                          |Adventure|Animation|Children|Comedy|Fantasy|Sci-Fi|
|7.3836784|111617 |Blended (2014)                                       |Comedy                                            |
|7.337799 |37384  |Waiting... (2005)                                    |Comedy                                            |
|7.0430093|955    |Bringing Up Baby (1938)                              |Comedy|Romance                                    |
|6.8849363|166461 |Moana (2016)                                         |Adventure|Animation|Children|Comedy|Fantasy       |
|6.8680916|70293  |Julie & Julia (2009)                                 |Comedy|Drama|Romance                              |
|6.849238 |8861   |Resident Evil: Apocalypse (2004)                     |Action|Horror|Sci-Fi|Thriller                     |
|6.8351765|505    |North (1994)                                         |Comedy                                            |
|6.7698655|4857   |Fiddler on the Roof (1971)                           |Drama|Musical                                     |
|6.724392 |69069  |Fired Up (2009)                                      |Comedy                                            |
|6.719549 |3100   |River Runs Through It, A (1992)                      |Drama                                             |
+---------+-------+-----------------------------------------------------+--------------------------------------------------+
