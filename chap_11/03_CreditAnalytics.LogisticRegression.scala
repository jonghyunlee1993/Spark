// *****************************************************************************
// 프로그램: 03_CreditAnalytics.LogisticRegression.scala
// 작성검증: Barnabas Kim(likebnb@gmail.com)
// 수정일자: 2020-05-23
// 배포버전: 1.0.2
// -----------------------------------------------------------------------------
// 모델유형: 분류(Classification)
// 알고리즘: 로지스틱회귀(LogisticRegression)
// 프로세스: 소스 > 특징변환 > 모델정의 > 파이프라인(학습,예측,평가) > 모델선택
// 모델평가: accuracy, RegressionMetrics
// *****************************************************************************
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.{ StringIndexer, VectorAssembler }
import org.apache.spark.ml.classification.{ LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.ml.tuning.{ ParamGridBuilder, CrossValidator }
import org.apache.spark.ml.{ Pipeline, PipelineModel, PipelineStage }

// *************************************
//  PHASE-1 : 데이터준비
// *************************************

// SQL Table -> DataFrame
// -------------------------------------
val creditDf = spark.sql("""
SELECT
    cast(substring(checking,-1) as Double) as checking,
    duration,
    cast(substring(history,-1) as Double) as history,
    cast(substring(purpose,-1) as Double) as purpose,
    amount,
    cast(substring(savings,-1) as Double) as savings,
    cast(substring(employment,-1) as Double) as employment,
    instRate,
    cast(substring(sexMarried,-1) as Double) as sexMarried,
    cast(substring(guarantors,-1) as Double) as guarantors,
    regiPeriod,
    cast(substring(property,-1) as Double) as property,
    age,
    cast(substring(otherInstPlan,-1) as Double) as otherInstPlan,
    cast(substring(housing,-1) as Double) as housing,
    existCredit,
    cast(substring(job,-1) as Double) as job,
    dependents,
    cast(substring(ownedPhone,-1) as Double) as ownedPhone,
    cast(substring(foreignWorker,-1) as Double) as foreignWorker,
    repaymentAbility
  FROM credit
""")

// 데이터프레임, creditDf 캐싱
// -------------------------------------
creditDf.show
creditDf.cache


// *************************************
//  PHASE-2 : 최초 모델훈련 및 검증
// *************************************

// 특징 선택
// -------------------------------------
val featureCols = Array(
        "checking","duration","history","purpose","amount","savings",
        "employment","instRate","sexMarried","guarantors","regiPeriod",
        "property","age","otherInstPlan","housing","existCredit","job",
        "dependents","ownedPhone","foreignWorker")

// 데이터분할(훈련셋 : 검증셋)
// -------------------------------------
val Array(trainSet, testSet) = creditDf.randomSplit(Array(0.8, 0.2), 5043L)

// 변환자 선언(라벨, 특징벡터)
// -------------------------------------
val labelIndexer = new StringIndexer().
        setInputCol("repaymentAbility").
        setOutputCol("label").
        fit(creditDf)

val featureAssembler = new VectorAssembler().
        setInputCols(featureCols).
        setOutputCol("features")

// 모델 선언(로지스틱회귀)
// -------------------------------------
val classifier = new LogisticRegression().
        setLabelCol("label").
        setFeaturesCol("features").
        setMaxIter(10).
        setRegParam(0.3).
        setElasticNetParam(0.8).
        setThreshold(0.2)

// 파이프라인, 모델적합
// -------------------------------------
val pipeline = new Pipeline().setStages(Array(labelIndexer, featureAssembler, classifier))
val model = pipeline.fit(trainSet)
val predictions = model.transform(testSet)

// 모델 검증(다중분류검증기)
// -------------------------------------
val evaluator = new MulticlassClassificationEvaluator().
        setLabelCol("label").
        setPredictionCol("prediction").
        setMetricName("accuracy")

val accuracy = evaluator.evaluate(predictions)


// 모델 검증과 메트릭(이진분류검증기)
// -------------------------------------
val binClassEval = new BinaryClassificationEvaluator().
        setLabelCol("label").
        setRawPredictionCol("rawPrediction")

val accuracy = binClassEval.evaluate(predictions)



// *************************************
//  PHASE-3 : 파라미터그리드와 교차검증
// *************************************

// 하이퍼파라미터 그리드
// -------------------------------------
val paramGrid = new ParamGridBuilder().
        addGrid(classifier.elasticNetParam, Array(0.5, 0.6, 0.8)).
        addGrid(classifier.fitIntercept,    Array(true, false)).
        addGrid(classifier.regParam,        Array(0.1, 0.3, 0.5)).
        addGrid(classifier.threshold,       Array(0.2, 0.32, 0.45)).
        build()

// 교차검증 선언
// -------------------------------------
val cv = new CrossValidator().
        setEstimator(pipeline).
        setEvaluator(evaluator).
        setEstimatorParamMaps(paramGrid).
        setNumFolds(5).
        setParallelism(5)

// 교차검증으로 모델적합
// -------------------------------------
val cvFittedModel = cv.fit(trainSet)
val predictions2 = cvFittedModel.transform(testSet)

// 모델검증
// -------------------------------------
val accuracy2 = evaluator.evaluate(predictions2)

// 최초모델과 교차검증모델 비교
// -------------------------------------
println(s"""
1st Model's Accuracy: ${accuracy}
 CV Model's Accuracy: ${accuracy2}
""")

// 교차검증의 베스트 모델 확인
// -------------------------------------
val plBestModel = cvFittedModel.
        bestModel.asInstanceOf[PipelineModel].
        stages(2)
plBestModel.extractParamMap

val lrBestModel = plBestModel.asInstanceOf[LogisticRegressionModel]
lrBestModel.extractParamMap

// *************************************
//  PHASE-4 : 모델의 세부지표 확인
// *************************************

// 초기모델과 교차검증 후 모델의 비교
// -------------------------------------
def printlnMetric(metricName: String, prediction: DataFrame): Double = {
    val metrics = binClassEval.setMetricName(metricName).evaluate(prediction)
    metrics
}

val rm = new RegressionMetrics(predictions.select("prediction", "label").rdd.
               map(x =>(x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))
val rm2 = new RegressionMetrics(predictions2.select("prediction", "label").rdd.
               map(x => (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))

println(s"""
---------------------------------------------------------------
1st Model Fitting & Validation
---------------------------------------------------------------
    Area Under ROC: ${printlnMetric("areaUnderROC", predictions)}
     Area Under PR: ${printlnMetric("areaUnderPR", predictions)}
               MSE: ${rm.meanSquaredError}
               MAE: ${rm.meanAbsoluteError}
              RMSE: ${rm.rootMeanSquaredError}
         R Squared: ${rm.r2}
Explained Variance: ${rm.explainedVariance}
---------------------------------------------------------------

---------------------------------------------------------------
Best Model after CrossValidation
---------------------------------------------------------------
    Area Under ROC: ${printlnMetric("areaUnderROC", predictions2)}
     Area Under PR: ${printlnMetric("areaUnderPR", predictions2)}
               MSE: ${rm2.meanSquaredError}
               MAE: ${rm2.meanAbsoluteError}
              RMSE: ${rm2.rootMeanSquaredError}
         R Squared: ${rm2.r2}
Explained Variance: ${rm2.explainedVariance}
---------------------------------------------------------------
""")

// 교차검증 후 최적화된 모델의 평가요약
// -------------------------------------
val lrSummary = lrBestModel.binarySummary
//lrSummary.roc.show()
//lrSummary.pr.show()
val fMeasure = lrSummary.fMeasureByThreshold
val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure).
                    select("threshold").head().getDouble(0)

println(s"""
---------------------------------------------------------------
LogisticRegressionModel's Summary
---------------------------------------------------------------
      Coefficients: ${lrBestModel.coefficients(0)}
         Intercept: ${lrBestModel.intercept}
          Accuracy: ${lrSummary.accuracy}
    Area Under ROC: ${lrSummary.areaUnderROC}
         F-Measure: ${maxFMeasure}
    Best Threshold: ${bestThreshold}
---------------------------------------------------------------
""")
