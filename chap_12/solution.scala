import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, DoubleType, FloatType}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.mllib.evaluation.{RankingMetrics, RegressionMetrics}
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}

spark.sql("""
CREATE TABLE movies (
    movieId        Integer     COMMENT '영화아이디',
    title          String      COMMENT '제목',
    genres         String      COMMENT '장르')
USING csv OPTIONS (
    header true,
    path '/Users/jonghyun/Workspace/SparkStudy/data/movies.csv')
""")

spark.sql("""
CREATE TABLE ratings (
    userId         String      COMMENT '사용자아이디',
    movieId        String      COMMENT '영화아이디',
    rating         Double      COMMENT '평점',
    timestamp      Long        COMMENT '타임스탬프')
USING csv OPTIONS (
    header true,
    path '/Users/jonghyun/Workspace/SparkStudy/data/ratings.csv')
""")

val ratingDf = spark.sql("""
SELECT cast(userId as Integer) as userId,
       cast(movieId as Integer) as movieId,
       cast(rating as Double) as rating
  FROM ratings
""")
ratingDf.cache

val userReviewCount = ratingDf.groupBy("userId").
       agg(count("userId").as("cnt")).sort($"cnt".desc)

// histogram 을 통한 구간 탐색
// val histogram = userReviewCount.select(col("cnt")).rdd.map(r => r.getLong(0)).histogram(5)

userReviewCount.where("cnt <= 30").count
userReviewCount.where("cnt > 30 and cnt <= 50").count
userReviewCount.where("cnt > 50 and cnt <= 80").count
userReviewCount.where("cnt > 80 and cnt <= 150").count
userReviewCount.where("cnt > 150 and cnt <= 300").count
userReviewCount.where("cnt > 300").count

val joinCond = ratingDf("userId")===userReviewCount("userId")
val joinType = "left_semi"
val userGroup1 = ratingDf.join(userReviewCount.where("cnt <= 30").toDF, joinCond, joinType)
val userGroup2 = ratingDf.join(userReviewCount.where("cnt > 30 and cnt <= 50").toDF, joinCond, joinType)
val userGroup3 = ratingDf.join(userReviewCount.where("cnt > 50 and cnt <= 80").toDF, joinCond, joinType)
val userGroup4 = ratingDf.join(userReviewCount.where("cnt > 80 and cnt <= 150").toDF, joinCond, joinType)
val userGroup5 = ratingDf.join(userReviewCount.where("cnt > 150 and cnt <= 300").toDF, joinCond, joinType)
val userGroup6 = ratingDf.join(userReviewCount.where("cnt > 300").toDF, joinCond, joinType)

// user_group_1
val Array(trainSet_user_group_1, testSet_user_group_1) = userGroup1.randomSplit(Array(0.8, 0.2), 5043L)
val recommender_user_group_1 = new ALS().
        setUserCol("userId").
        setItemCol("movieId").
        setRatingCol("rating").
        setMaxIter(3).
        setRegParam(0.01).
        setColdStartStrategy("drop").
        setNonnegative(false).
        setImplicitPrefs(false).
        setSeed(1234567)
val pipeline_user_group_1    = new Pipeline().setStages(Array(recommender_user_group_1))
val model_user_group_1       = pipeline_user_group_1.fit(trainSet_user_group_1)
val predictions_user_group_1 = model_user_group_1.transform(testSet_user_group_1)
val evaluator_user_group_1 = new RegressionEvaluator().
        setLabelCol("rating").
        setPredictionCol("prediction").
        setMetricName("rmse")
val rmse_user_group_1 = evaluator_user_group_1.evaluate(predictions_user_group_1)


// user_group_2
val Array(trainSet_user_group_2, testSet_user_group_2) = userGroup2.randomSplit(Array(0.8, 0.2), 5043L)
val recommender_user_group_2 = new ALS().
        setUserCol("userId").
        setItemCol("movieId").
        setRatingCol("rating").
        setMaxIter(3).
        setRegParam(0.01).
        setColdStartStrategy("drop").
        setNonnegative(false).
        setImplicitPrefs(false).
        setSeed(1234567)
val pipeline_user_group_2    = new Pipeline().setStages(Array(recommender_user_group_2))
val model_user_group_2       = pipeline_user_group_2.fit(trainSet_user_group_2)
val predictions_user_group_2 = model_user_group_2.transform(testSet_user_group_2)
val evaluator_user_group_2 = new RegressionEvaluator().
        setLabelCol("rating").
        setPredictionCol("prediction").
        setMetricName("rmse")
val rmse_user_group_2 = evaluator_user_group_2.evaluate(predictions_user_group_2)


// user_group_3
val Array(trainSet_user_group_3, testSet_user_group_3) = userGroup3.randomSplit(Array(0.8, 0.2), 5043L)
val recommender_user_group_3 = new ALS().
        setUserCol("userId").
        setItemCol("movieId").
        setRatingCol("rating").
        setMaxIter(3).
        setRegParam(0.01).
        setColdStartStrategy("drop").
        setNonnegative(false).
        setImplicitPrefs(false).
        setSeed(1234567)
val pipeline_user_group_3    = new Pipeline().setStages(Array(recommender_user_group_3))
val model_user_group_3       = pipeline_user_group_3.fit(trainSet_user_group_3)
val predictions_user_group_3 = model_user_group_3.transform(testSet_user_group_3)
val evaluator_user_group_3 = new RegressionEvaluator().
        setLabelCol("rating").
        setPredictionCol("prediction").
        setMetricName("rmse")
val rmse_user_group_3 = evaluator_user_group_3.evaluate(predictions_user_group_3)


// user_group_4
val Array(trainSet_user_group_4, testSet_user_group_4) = userGroup4.randomSplit(Array(0.8, 0.2), 5043L)
val recommender_user_group_4 = new ALS().
        setUserCol("userId").
        setItemCol("movieId").
        setRatingCol("rating").
        setMaxIter(3).
        setRegParam(0.01).
        setColdStartStrategy("drop").
        setNonnegative(false).
        setImplicitPrefs(false).
        setSeed(1234567)
val pipeline_user_group_4    = new Pipeline().setStages(Array(recommender_user_group_4))
val model_user_group_4       = pipeline_user_group_4.fit(trainSet_user_group_4)
val predictions_user_group_4 = model_user_group_4.transform(testSet_user_group_4)
val evaluator_user_group_4 = new RegressionEvaluator().
        setLabelCol("rating").
        setPredictionCol("prediction").
        setMetricName("rmse")
val rmse_user_group_4 = evaluator_user_group_4.evaluate(predictions_user_group_4)


// user_group_5
val Array(trainSet_user_group_5, testSet_user_group_5) = userGroup5.randomSplit(Array(0.8, 0.2), 5043L)
val recommender_user_group_5 = new ALS().
        setUserCol("userId").
        setItemCol("movieId").
        setRatingCol("rating").
        setMaxIter(3).
        setRegParam(0.01).
        setColdStartStrategy("drop").
        setNonnegative(false).
        setImplicitPrefs(false).
        setSeed(1234567)
val pipeline_user_group_5    = new Pipeline().setStages(Array(recommender_user_group_5))
val model_user_group_5       = pipeline_user_group_5.fit(trainSet_user_group_5)
val predictions_user_group_5 = model_user_group_5.transform(testSet_user_group_5)
val evaluator_user_group_5 = new RegressionEvaluator().
        setLabelCol("rating").
        setPredictionCol("prediction").
        setMetricName("rmse")
val rmse_user_group_5 = evaluator_user_group_5.evaluate(predictions_user_group_5)


// user_group_6
val Array(trainSet_user_group_6, testSet_user_group_6) = userGroup6.randomSplit(Array(0.8, 0.2), 5043L)
val recommender_user_group_6 = new ALS().
        setUserCol("userId").
        setItemCol("movieId").
        setRatingCol("rating").
        setMaxIter(3).
        setRegParam(0.01).
        setColdStartStrategy("drop").
        setNonnegative(false).
        setImplicitPrefs(false).
        setSeed(1234567)
val pipeline_user_group_6    = new Pipeline().setStages(Array(recommender_user_group_6))
val model_user_group_6       = pipeline_user_group_6.fit(trainSet_user_group_6)
val predictions_user_group_6 = model_user_group_6.transform(testSet_user_group_6)
val evaluator_user_group_6 = new RegressionEvaluator().
        setLabelCol("rating").
        setPredictionCol("prediction").
        setMetricName("rmse")
val rmse_user_group_6 = evaluator_user_group_6.evaluate(predictions_user_group_6)


// print RMSE results
println(s"""
    1st Group's RMSE: ${rmse_user_group_1}
    2nd Group's RMSE: ${rmse_user_group_2}
    3rd Group's RMSE: ${rmse_user_group_3}
    4th Group's RMSE: ${rmse_user_group_4}
    5th Group's RMSE: ${rmse_user_group_5}
    6th Group's RMSE: ${rmse_user_group_6}
""")


// cold start 조건 필터링
val cond_1 = ratingDf.join(userReviewCount.where("cnt > 30").toDF, joinCond, joinType)
val cond_2 = ratingDf.join(userReviewCount.where("cnt > 50").toDF, joinCond, joinType)
val cond_3 = ratingDf.join(userReviewCount.where("cnt > 80").toDF, joinCond, joinType)
val cond_4 = ratingDf.join(userReviewCount.where("cnt > 150").toDF, joinCond, joinType)
val cond_5 = ratingDf.join(userReviewCount.where("cnt > 300").toDF, joinCond, joinType)

// cond_1
val Array(trainSet_cond_1, testSet_cond_1) = cond_1.randomSplit(Array(0.8, 0.2), 5043L)
val recommender_cond_1 = new ALS().
        setUserCol("userId").
        setItemCol("movieId").
        setRatingCol("rating").
        setMaxIter(3).
        setRegParam(0.01).
        setColdStartStrategy("drop").
        setNonnegative(false).
        setImplicitPrefs(false).
        setSeed(1234567)
val pipeline_cond_1    = new Pipeline().setStages(Array(recommender_cond_1))
val model_cond_1       = pipeline_cond_1.fit(trainSet_cond_1)
val predictions_cond_1 = model_cond_1.transform(testSet_cond_1)
val evaluator_cond_1 = new RegressionEvaluator().
        setLabelCol("rating").
        setPredictionCol("prediction").
        setMetricName("rmse")
val rmse_cond_1 = evaluator_cond_1.evaluate(predictions_cond_1)


// cond_2
val Array(trainSet_cond_2, testSet_cond_2) = cond_2.randomSplit(Array(0.8, 0.2), 5043L)
val recommender_cond_2 = new ALS().
        setUserCol("userId").
        setItemCol("movieId").
        setRatingCol("rating").
        setMaxIter(3).
        setRegParam(0.01).
        setColdStartStrategy("drop").
        setNonnegative(false).
        setImplicitPrefs(false).
        setSeed(1234567)
val pipeline_cond_2    = new Pipeline().setStages(Array(recommender_cond_2))
val model_cond_2       = pipeline_cond_2.fit(trainSet_cond_2)
val predictions_cond_2 = model_cond_2.transform(testSet_cond_2)
val evaluator_cond_2 = new RegressionEvaluator().
        setLabelCol("rating").
        setPredictionCol("prediction").
        setMetricName("rmse")
val rmse_cond_2 = evaluator_cond_2.evaluate(predictions_cond_2)


// cond_3
val Array(trainSet_cond_3, testSet_cond_3) = cond_3.randomSplit(Array(0.8, 0.2), 5043L)
val recommender_cond_3 = new ALS().
        setUserCol("userId").
        setItemCol("movieId").
        setRatingCol("rating").
        setMaxIter(3).
        setRegParam(0.01).
        setColdStartStrategy("drop").
        setNonnegative(false).
        setImplicitPrefs(false).
        setSeed(1234567)
val pipeline_cond_3    = new Pipeline().setStages(Array(recommender_cond_3))
val model_cond_3       = pipeline_cond_3.fit(trainSet_cond_3)
val predictions_cond_3 = model_cond_3.transform(testSet_cond_3)
val evaluator_cond_3 = new RegressionEvaluator().
        setLabelCol("rating").
        setPredictionCol("prediction").
        setMetricName("rmse")
val rmse_cond_3 = evaluator_cond_3.evaluate(predictions_cond_3)


// cond_4
val Array(trainSet_cond_4, testSet_cond_4) = cond_4.randomSplit(Array(0.8, 0.2), 5043L)
val recommender_cond_4 = new ALS().
        setUserCol("userId").
        setItemCol("movieId").
        setRatingCol("rating").
        setMaxIter(3).
        setRegParam(0.01).
        setColdStartStrategy("drop").
        setNonnegative(false).
        setImplicitPrefs(false).
        setSeed(1234567)
val pipeline_cond_4    = new Pipeline().setStages(Array(recommender_cond_4))
val model_cond_4       = pipeline_cond_4.fit(trainSet_cond_4)
val predictions_cond_4 = model_cond_4.transform(testSet_cond_4)
val evaluator_cond_4 = new RegressionEvaluator().
        setLabelCol("rating").
        setPredictionCol("prediction").
        setMetricName("rmse")
val rmse_cond_4 = evaluator_cond_4.evaluate(predictions_cond_4)


// cond_5
val Array(trainSet_cond_5, testSet_cond_5) = cond_5.randomSplit(Array(0.8, 0.2), 5043L)
val recommender_cond_5 = new ALS().
        setUserCol("userId").
        setItemCol("movieId").
        setRatingCol("rating").
        setMaxIter(3).
        setRegParam(0.01).
        setColdStartStrategy("drop").
        setNonnegative(false).
        setImplicitPrefs(false).
        setSeed(1234567)
val pipeline_cond_5    = new Pipeline().setStages(Array(recommender_cond_5))
val model_cond_5       = pipeline_cond_5.fit(trainSet_cond_5)
val predictions_cond_5 = model_cond_5.transform(testSet_cond_5)
val evaluator_cond_5 = new RegressionEvaluator().
        setLabelCol("rating").
        setPredictionCol("prediction").
        setMetricName("rmse")
val rmse_cond_5 = evaluator_cond_5.evaluate(predictions_cond_5)


// print RMSE results
println(s"""
    1st condition's RMSE: ${rmse_cond_1}
    2nd condition's RMSE: ${rmse_cond_2}
    3rd condition's RMSE: ${rmse_cond_3}
    4th condition's RMSE: ${rmse_cond_4}
    5th condition's RMSE: ${rmse_cond_5}
""")


// minimum search
val cond_1_minimum = ratingDf.join(userReviewCount.where("cnt > 5").toDF, joinCond, joinType)
val cond_2_minimum = ratingDf.join(userReviewCount.where("cnt > 10").toDF, joinCond, joinType)
val cond_3_minimum = ratingDf.join(userReviewCount.where("cnt > 20").toDF, joinCond, joinType)
val cond_4_minimum = ratingDf.join(userReviewCount.where("cnt > 30").toDF, joinCond, joinType)
val cond_5_minimum = ratingDf.join(userReviewCount.where("cnt > 40").toDF, joinCond, joinType)
val cond_6_minimum = ratingDf.join(userReviewCount.where("cnt > 50").toDF, joinCond, joinType)

// cond_1_minimum
val Array(trainSet_cond_1_minimum, testSet_cond_1_minimum) = cond_1_minimum.randomSplit(Array(0.8, 0.2), 5043L)
val recommender_cond_1_minimum = new ALS().
        setUserCol("userId").
        setItemCol("movieId").
        setRatingCol("rating").
        setMaxIter(3).
        setRegParam(0.01).
        setColdStartStrategy("drop").
        setNonnegative(false).
        setImplicitPrefs(false).
        setSeed(1234567)
val pipeline_cond_1_minimum    = new Pipeline().setStages(Array(recommender_cond_1_minimum))
val model_cond_1_minimum       = pipeline_cond_1_minimum.fit(trainSet_cond_1_minimum)
val predictions_cond_1_minimum = model_cond_1_minimum.transform(testSet_cond_1_minimum)
val evaluator_cond_1_minimum = new RegressionEvaluator().
        setLabelCol("rating").
        setPredictionCol("prediction").
        setMetricName("rmse")
val rmse_cond_1_minimum = evaluator_cond_1_minimum.evaluate(predictions_cond_1_minimum)


// cond_2_minimum
val Array(trainSet_cond_2_minimum, testSet_cond_2_minimum) = cond_2_minimum.randomSplit(Array(0.8, 0.2), 5043L)
val recommender_cond_2_minimum = new ALS().
        setUserCol("userId").
        setItemCol("movieId").
        setRatingCol("rating").
        setMaxIter(3).
        setRegParam(0.01).
        setColdStartStrategy("drop").
        setNonnegative(false).
        setImplicitPrefs(false).
        setSeed(1234567)
val pipeline_cond_2_minimum    = new Pipeline().setStages(Array(recommender_cond_2_minimum))
val model_cond_2_minimum       = pipeline_cond_2_minimum.fit(trainSet_cond_2_minimum)
val predictions_cond_2_minimum = model_cond_2_minimum.transform(testSet_cond_2_minimum)
val evaluator_cond_2_minimum = new RegressionEvaluator().
        setLabelCol("rating").
        setPredictionCol("prediction").
        setMetricName("rmse")
val rmse_cond_2_minimum = evaluator_cond_2_minimum.evaluate(predictions_cond_2_minimum)


// cond_3_minimum
val Array(trainSet_cond_3_minimum, testSet_cond_3_minimum) = cond_3_minimum.randomSplit(Array(0.8, 0.2), 5043L)
val recommender_cond_3_minimum = new ALS().
        setUserCol("userId").
        setItemCol("movieId").
        setRatingCol("rating").
        setMaxIter(3).
        setRegParam(0.01).
        setColdStartStrategy("drop").
        setNonnegative(false).
        setImplicitPrefs(false).
        setSeed(1234567)
val pipeline_cond_3_minimum    = new Pipeline().setStages(Array(recommender_cond_3_minimum))
val model_cond_3_minimum       = pipeline_cond_3_minimum.fit(trainSet_cond_3_minimum)
val predictions_cond_3_minimum = model_cond_3_minimum.transform(testSet_cond_3_minimum)
val evaluator_cond_3_minimum = new RegressionEvaluator().
        setLabelCol("rating").
        setPredictionCol("prediction").
        setMetricName("rmse")
val rmse_cond_3_minimum = evaluator_cond_3_minimum.evaluate(predictions_cond_3_minimum)


// cond_4_minimum
val Array(trainSet_cond_4_minimum, testSet_cond_4_minimum) = cond_4_minimum.randomSplit(Array(0.8, 0.2), 5043L)
val recommender_cond_4_minimum = new ALS().
        setUserCol("userId").
        setItemCol("movieId").
        setRatingCol("rating").
        setMaxIter(3).
        setRegParam(0.01).
        setColdStartStrategy("drop").
        setNonnegative(false).
        setImplicitPrefs(false).
        setSeed(1234567)
val pipeline_cond_4_minimum    = new Pipeline().setStages(Array(recommender_cond_4_minimum))
val model_cond_4_minimum       = pipeline_cond_4_minimum.fit(trainSet_cond_4_minimum)
val predictions_cond_4_minimum = model_cond_4_minimum.transform(testSet_cond_4_minimum)
val evaluator_cond_4_minimum = new RegressionEvaluator().
        setLabelCol("rating").
        setPredictionCol("prediction").
        setMetricName("rmse")
val rmse_cond_4_minimum = evaluator_cond_4_minimum.evaluate(predictions_cond_4_minimum)


// cond_5_minimum
val Array(trainSet_cond_5_minimum, testSet_cond_5_minimum) = cond_5_minimum.randomSplit(Array(0.8, 0.2), 5043L)
val recommender_cond_5_minimum = new ALS().
        setUserCol("userId").
        setItemCol("movieId").
        setRatingCol("rating").
        setMaxIter(3).
        setRegParam(0.01).
        setColdStartStrategy("drop").
        setNonnegative(false).
        setImplicitPrefs(false).
        setSeed(1234567)
val pipeline_cond_5_minimum    = new Pipeline().setStages(Array(recommender_cond_5_minimum))
val model_cond_5_minimum       = pipeline_cond_5_minimum.fit(trainSet_cond_5_minimum)
val predictions_cond_5_minimum = model_cond_5_minimum.transform(testSet_cond_5_minimum)
val evaluator_cond_5_minimum = new RegressionEvaluator().
        setLabelCol("rating").
        setPredictionCol("prediction").
        setMetricName("rmse")
val rmse_cond_5_minimum = evaluator_cond_5_minimum.evaluate(predictions_cond_5_minimum)


// cond_6_minimum
val Array(trainSet_cond_6_minimum, testSet_cond_6_minimum) = cond_6_minimum.randomSplit(Array(0.8, 0.2), 5043L)
val recommender_cond_6_minimum = new ALS().
        setUserCol("userId").
        setItemCol("movieId").
        setRatingCol("rating").
        setMaxIter(3).
        setRegParam(0.01).
        setColdStartStrategy("drop").
        setNonnegative(false).
        setImplicitPrefs(false).
        setSeed(1234567)
val pipeline_cond_6_minimum    = new Pipeline().setStages(Array(recommender_cond_6_minimum))
val model_cond_6_minimum       = pipeline_cond_6_minimum.fit(trainSet_cond_6_minimum)
val predictions_cond_6_minimum = model_cond_6_minimum.transform(testSet_cond_6_minimum)
val evaluator_cond_6_minimum = new RegressionEvaluator().
        setLabelCol("rating").
        setPredictionCol("prediction").
        setMetricName("rmse")
val rmse_cond_6_minimum = evaluator_cond_6_minimum.evaluate(predictions_cond_6_minimum)


// print RMSE results
println(s"""
    1st condition's RMSE: ${rmse_cond_1_minimum}
    2nd condition's RMSE: ${rmse_cond_2_minimum}
    3rd condition's RMSE: ${rmse_cond_3_minimum}
    4th condition's RMSE: ${rmse_cond_4_minimum}
    5th condition's RMSE: ${rmse_cond_5_minimum}
    6th condition's RMSE: ${rmse_cond_6_minimum}
""")