// 데이터 로드

spark.sql("""
CREATE TABLE credit (
    checking         String    COMMENT '당좌예금',
    duration         Double    COMMENT '대출기간',
    history          String    COMMENT '부실대출이력',
    purpose          String    COMMENT '대출목적',
    amount           Double    COMMENT '신청금액',
    savings          String    COMMENT '저축예금',
    employment       String    COMMENT '고용상태',
    instRate         Double    COMMENT '소득대비상환비율',
    sexMarried       String    COMMENT '성별과결혼상태',
    guarantors       String    COMMENT '보증인',
    regiPeriod       Double    COMMENT '거주기간',
    property         String    COMMENT '재산',
    age              Double    COMMENT '연령',
    otherInstPlan    String    COMMENT '다른상환계획',
    housing          String    COMMENT '주거상태',
    existCredit      Double    COMMENT '기존대출',
    job              String    COMMENT '직업',
    dependents       Double    COMMENT '부양가족',
    ownedPhone       String    COMMENT '전화소유',
    foreignWorker    String    COMMENT '외국인근로자',
    repaymentAbility Double    COMMENT '상환능력')
 USING csv OPTIONS (
    header true,
    path '/Users/jonghyun/Workspace/SparkStudy/data/german_credit_1994.csv')
""")

spark.sql("""
SELECT repaymentAbility, avg(amount), avg(duration),
       avg(regiPeriod), avg(existCredit), avg(dependents)
  FROM credit
 GROUP BY repaymentAbility
""").show

spark.sql("select distinct checking from credit").show
spark.sql("select distinct history from credit").show
spark.sql("select distinct purpose from credit").show
spark.sql("select distinct savings from credit").show
spark.sql("select distinct employment from credit").show
spark.sql("select distinct sexMarried from credit").show
spark.sql("select distinct guarantors from credit").show
spark.sql("select distinct property from credit").show
spark.sql("select distinct otherInstPlan from credit").show
spark.sql("select distinct housing from credit").show
spark.sql("select distinct job from credit").show
spark.sql("select distinct ownedPhone from credit").show
spark.sql("select distinct foreignWorker from credit").show

// 모델링
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.{ StringIndexer, VectorAssembler}
import org.apache.spark.ml.classification.{ RandomForestClassifier, RandomForestClassificationModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.ml.tuning.{ ParamGridBuilder, CrossValidator }
import org.apache.spark.ml.{ Pipeline, PipelineModel, PipelineStage }

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

creditDf.cache

// 특징 선택
// -------------------------------------
val featureCols = Array(
        "checking","duration","history","purpose","amount","savings",
        "employment","instRate","sexMarried","guarantors","regiPeriod",
        "property","age","otherInstPlan","housing","existCredit","job",
        "dependents","ownedPhone","foreignWorker")

// 데이터분할(훈련셋:검증셋 = 8:2)
// -------------------------------------
val Array(trainSet, testSet) = creditDf.randomSplit(Array(0.8, 0.2), 5043L)

// 변환자 선언(라벨, 특징벡터)
// -------------------------------------
val labelIndexer = new StringIndexer().
        setInputCol("repaymentAbility").
        setOutputCol("label")

val featureAssembler = new VectorAssembler().
        setInputCols(featureCols).
        setOutputCol("features")

// 모델 선언(랜덤포레스트)
// -------------------------------------
val classifier = new RandomForestClassifier().
        setImpurity("gini").
        setMaxDepth(5).
        setNumTrees(5).
        setMaxBins(5).
        setMinInfoGain(0.001).
        setFeatureSubsetStrategy("auto").
        setSeed(1234567)

// 파이프라인, 모델적합
// -------------------------------------
val pipeline = new Pipeline().
        setStages(Array(
            labelIndexer,
            featureAssembler,
            classifier
        ))
val model = pipeline.fit(trainSet)
val predictions = model.transform(testSet)

// 모델 검증과 메트릭
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
        addGrid(classifier.maxDepth, Array(5, 7, 9, 10, 15, 20)).
        addGrid(classifier.numTrees, Array(30, 50, 70, 100)).
        addGrid(classifier.maxBins,  Array(25, 30, 40, 50)).
        addGrid(classifier.impurity, Array("entropy", "gini")).
        build()

// 교차검증 선언
// -------------------------------------
val cv = new CrossValidator().
        setEstimator(pipeline).
        setEstimatorParamMaps(paramGrid).
        setEvaluator(binClassEval).
        setNumFolds(5).
        setParallelism(5)

// 교차검증으로 모델적합
// -------------------------------------
val cvFittedModel = cv.fit(trainSet)
val predictions2 = cvFittedModel.transform(testSet)

// 모델검증
// -------------------------------------
val accuracy2 = binClassEval.evaluate(predictions2)

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

val rfcModel = plBestModel.asInstanceOf[RandomForestClassificationModel]
rfcModel.extractParamMap
rfcModel.toDebugString

// *************************************
//  PHASE-4 : 각 모델의 세부지표 확인
// *************************************

// 초기모델과 교차검증 후 모델의 비교
// -------------------------------------
def printlnMetric(metricName: String, prediction: DataFrame): Double = {
    val metrics = binClassEval.setMetricName(metricName).evaluate(prediction)
    metrics
}

val rm = new RegressionMetrics(predictions.select("prediction", "label").rdd.
               map(x => (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))
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
