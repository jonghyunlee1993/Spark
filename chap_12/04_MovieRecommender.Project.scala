// *****************************************************************************
// 프로그램: 04_MovieRecommender.Project.scala
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


// https://koasas.kaist.ac.kr/bitstream/10203/22063/1/4284.pdf
// ---------------------------------------------------------------

// 리뷰횟수 구간으로 사용자 그룹 구분
// -------------------------------------
val userReviewCount = ratingDf.groupBy("userId").
       agg(count("userId").as("cnt")).sort($"cnt".desc)
// -------------------------------------
userReviewCount.where("cnt <= 30").count
userReviewCount.where("cnt > 30 and cnt <= 50").count
userReviewCount.where("cnt > 50 and cnt <= 80").count
userReviewCount.where("cnt > 80 and cnt <= 150").count
userReviewCount.where("cnt > 150 and cnt <= 300").count
userReviewCount.where("cnt > 300").count
// -------------------------------------
val joinCond = ratingDf("userId")===userReviewCount("userId")
val joinType = "left_semi"
val userGroup1 = ratingDf.join(userReviewCount.where("cnt <= 30").toDF, joinCond, joinType)
val userGroup2 = ratingDf.join(userReviewCount.where("cnt > 30 and cnt <= 50").toDF, joinCond, joinType)
val userGroup3 = ratingDf.join(userReviewCount.where("cnt > 50 and cnt <= 80").toDF, joinCond, joinType)
val userGroup4 = ratingDf.join(userReviewCount.where("cnt > 80 and cnt <= 150").toDF, joinCond, joinType)
val userGroup5 = ratingDf.join(userReviewCount.where("cnt > 150 and cnt <= 300").toDF, joinCond, joinType)
val userGroup6 = ratingDf.join(userReviewCount.where("cnt > 300").toDF, joinCond, joinType)
