import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.{BisectingKMeans, BisectingKMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}

// *************************************
//  PHASE-1 : 데이터준비
// *************************************

// SQL Table -> DataFrame
// -------------------------------------
val creditDf = spark.sql("""SELECT
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

// 변환자 선언(특징벡터)
// -------------------------------------
val featureAssembler = new VectorAssembler().
        setInputCols(featureCols).
        setOutputCol("features")

// 모델 선언(BisectingKMeans)
// -------------------------------------
val bs = new BisectingKMeans().
        setK(10).
        setMaxIter(5).
        setMinDivisibleClusterSize(1.0).
        setSeed(1L)

// 파이프라인, 모델적합
// -------------------------------------
val pipeline = new Pipeline().setStages(Array(featureAssembler, bs))
val model = pipeline.fit(creditDf)
val predictions = model.transform(creditDf)

// 모델 추출
// -------------------------------------
val bsModel = model.stages(1).asInstanceOf[BisectingKMeansModel]
bsModel.extractParamMap

// 모델 평가: 계산비용
// -------------------------------------
val cost = bsModel.computeCost(predictions)

predictions.
       select("repaymentAbility", "features", "prediction").
       where("prediction = 0").show(false)

// 모델 평가: 실루엣
// -------------------------------------
val evaluator = new ClusteringEvaluator()
val silhouette = evaluator.evaluate(predictions)
println(s""" 1에 가까운 값이 최적
1st Model's Silhouette: ${silhouette}
""")

// *************************************
//  PHASE-3 : 파라미터그리드와 교차검증
// *************************************

// 하이퍼파라미터 그리드
// -------------------------------------
val paramGrid = new ParamGridBuilder().
        addGrid(bs.k, Array(2, 5)).
        addGrid(bs.maxIter, Array(10, 20)).
        addGrid(bs.minDivisibleClusterSize,  Array(2.0, 5.0)).
        build()

// 교차검증 선언
// -------------------------------------
val cv = new CrossValidator().
        setEstimator(pipeline).
        setEvaluator(evaluator).
        setEstimatorParamMaps(paramGrid).
        setNumFolds(2)

// 교차검증 모델적합
// -------------------------------------
val cvFittedModel = cv.fit(creditDf)
val predictions2 = cvFittedModel.transform(creditDf)

// 모델검증
// -------------------------------------
val silhouette2 = evaluator.evaluate(predictions2)

// 최초모델과 교차검증모델 비교
// -------------------------------------
println(s""" 1에 가까운 값이 최적
1st Model's Silhouette: ${silhouette}
 CV Model's Silhouette: ${silhouette2}
""")

// 교차검증의 베스트 모델 확인
// -------------------------------------
val bestModel = cvFittedModel.
        bestModel.asInstanceOf[PipelineModel].
        stages(1)
bestModel.extractParamMap

// PipelineModel과 결과 같음
// -------------------------------------
val cvModel = bestModel.asInstanceOf[BisectingKMeansModel]
cvModel.extractParamMap
