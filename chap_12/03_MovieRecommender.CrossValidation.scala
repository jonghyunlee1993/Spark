// *****************************************************************************
// 프로그램: 03_MovieRecommender.CrossValidation.scala
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
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}

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
//  PHASE-2 : 최초 모델훈련 및 검증
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
        setColdStartStrategy("drop").
        setNonnegative(false).
        setImplicitPrefs(false).
        setSeed(1234567)

// 파이프라인, 모델적합
// -------------------------------------
val pipeline = new Pipeline().setStages(Array(recommender))
val model = pipeline.fit(trainSet)
val predictions = model.transform(testSet)

// 모델 검증
// -------------------------------------
val evaluator = new RegressionEvaluator().
        setLabelCol("rating").
        setPredictionCol("prediction").
        setMetricName("rmse")

// 평가지표1: RMSE(평균제곱근오차)
// -------------------------------------
val rmse = evaluator.evaluate(predictions)
println(s"""
1st Model's RMSE: ${rmse}
""")


// *************************************
//  PHASE-3 : 파라미터그리드와 교차검증
// *************************************
// 하이퍼파라미터 그리드
// -------------------------------------
// 15분 20초
val paramGrid = new ParamGridBuilder().
        addGrid(recommender.implicitPrefs, Array(true, false)).   // 암묵적 데이터 여부
        addGrid(recommender.alpha,         Array(0.5, 1.0, 1.5)). // 사용자 기본신뢰도
        addGrid(recommender.rank,          Array(5, 10, 20)).     // 특징벡터 차원
        addGrid(recommender.regParam,      Array(0.05, 0.1, 0.15)).// 일반화 정도
        // addGrid(recommender.maxIter,       Array(3, 20)).         // 제일 나중에 튜닝
        // addGrid(recommender.nonnegative,   Array(true, false)).   // 음수값이 없으면(x)
        build()

// 3분 15초
val paramGrid = new ParamGridBuilder().
        addGrid(recommender.implicitPrefs, Array(true, false)).   // 암묵적 데이터 여부
        addGrid(recommender.alpha,         Array(0.5, 1.0)).      // 사용자 기본신뢰도
        addGrid(recommender.rank,          Array(5, 10)).         // 특징벡터 차원
        addGrid(recommender.regParam,      Array(0.05, 0.1)).     // 일반화 정도
        build()

// 교차검증 선언
// -------------------------------------
val cv = new CrossValidator().
        setEstimator(pipeline).
        setEstimatorParamMaps(paramGrid).
        setEvaluator(evaluator).
        setNumFolds(5).
        setParallelism(12)

// 교차검증 모델적합 및 예측
// -------------------------------------
val cvFittedModel = cv.fit(trainSet)
val predictions2 = cvFittedModel.transform(testSet)

// 평가지표2: RMSE
// -------------------------------------
val rmse2 = evaluator.evaluate(predictions2)

// 최초모델과 교차검증모델 비교
// -------------------------------------
println(s"""
1st Model's RMSE: ${rmse}
 CV Model's RMSE: ${rmse2}
""")

// 교차검증의 베스트 모델 확인
// -------------------------------------
val plBestModel = cvFittedModel.
        bestModel.asInstanceOf[PipelineModel].
        stages(0)

val alsModel = plBestModel.asInstanceOf[ALSModel]
alsModel.extractParamMap

// 회귀평가지표 비교
// -------------------------------------
val rm = new RegressionMetrics(
                 predictions.select("rating", "prediction").
                 rdd.map(x => (x.getDouble(0).toFloat, x.getFloat(1))))

val rm2 = new RegressionMetrics(
                  predictions2.select("rating", "prediction").
                  rdd.map(x => (x.getDouble(0).toFloat, x.getFloat(1))))

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

---------------------------------------------------------------
Best Model after Cross Validation
---------------------------------------------------------------
               MSE: ${rm2.meanSquaredError}
               MAE: ${rm2.meanAbsoluteError}
              RMSE: ${rm2.rootMeanSquaredError}
         R Squared: ${rm2.r2}
Explained Variance: ${rm2.explainedVariance}
---------------------------------------------------------------
""")


// *************************************
//  PHASE-4 : 순위평가지표
// *************************************
// 사용자별 실제 평점(2.5 초과) 목록
// -------------------------------------
val perUserActual = predictions2.
        where("rating > 2.5").
        groupBy("userId").
        agg(expr("collect_list(movieId) as movies"))

// 사용자별 평점 예측값 목록
// -------------------------------------
val perUserPredictions = predictions2.
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
val userMovies = predictions2.where("userId = 159").where("rating > 2.5").
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

// 추천받은 영화아이디로 영화정보 조인
// ----------------------------------------------
val joinDf2 = rcmdMovies.join(movieDf,
      movieDf("movieId") === rcmdMovies("mId"), "inner").
      select("rating", "movieId", "title", "genres")

joinDf2.show(false)
+---------+-------+------------------------------------------------------------------+------------------------------------------+
|rating   |movieId|title                                                             |genres                                    |
+---------+-------+------------------------------------------------------------------+------------------------------------------+
|4.8474383|94070  |Best Exotic Marigold Hotel, The (2011)                            |Comedy|Drama                              |
|4.6792994|6732   |Hello, Dolly! (1969)                                              |Comedy|Musical|Romance                    |
|4.595525 |26183  |Asterix and Cleopatra (Astérix et Cléopâtre) (1968)               |Action|Adventure|Animation|Children|Comedy|
|4.5720105|25906  |Mr. Skeffington (1944)                                            |Drama|Romance                             |
|4.5720105|77846  |12 Angry Men (1997)                                               |Crime|Drama                               |
|4.5720105|93008  |Very Potter Sequel, A (2010)                                      |Comedy|Musical                            |
|4.5618434|66943  |Cottage, The (2008)                                               |Comedy|Crime|Horror|Thriller              |
|4.546552 |33090  |Mutant Aliens (2001)                                              |Animation|Comedy|Sci-Fi                   |
|4.544046 |131610 |Willy/Milly (1986)                                                |Comedy|Fantasy                            |
|4.544046 |44851  |Go for Zucker! (Alles auf Zucker!) (2004)                         |Comedy                                    |
|4.544046 |143031 |Jump In! (2007)                                                   |Comedy|Drama|Romance                      |
|4.544046 |136341 |Scooby-Doo! and the Samurai Sword (2009)                          |Animation|Children|Comedy                 |
|4.544046 |73822  |Meantime (1984)                                                   |Comedy|Drama                              |
|4.544046 |109241 |On the Other Side of the Tracks (De l'autre côté du périph) (2012)|Action|Comedy|Crime                       |
|4.544046 |139640 |Ooops! Noah is Gone... (2015)                                     |Animation                                 |
|4.544046 |147410 |A Perfect Day (2015)                                              |Comedy|Drama                              |
|4.544046 |126921 |The Fox and the Hound 2 (2006)                                    |Adventure|Animation|Children|Comedy       |
|4.544046 |53280  |Breed, The (2006)                                                 |Horror|Thriller                           |
|4.544046 |118270 |Hellbenders (2012)                                                |Comedy|Horror|Thriller                    |
|4.544046 |131130 |Tom and Jerry: A Nutcracker Tale (2007)                           |Animation|Comedy                          |
+---------+-------+------------------------------------------------------------------+------------------------------------------+
