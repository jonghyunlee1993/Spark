import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
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

// 모델 선언(KMeans)
// -------------------------------------
val km = new KMeans().
        setK(10).
        setInitMode("k-means||").
        setInitSteps(2).
        setMaxIter(5).
        setTol(0.01).
        setSeed(1L)

// 파이프라인, 모델적합
// -------------------------------------
val pipeline = new Pipeline().setStages(Array(featureAssembler, km))
val model = pipeline.fit(creditDf)
val predictions = model.transform(creditDf)

// 모델 평가: 훈련비용
// -------------------------------------
val kmModel = model.stages(1).asInstanceOf[KMeansModel]
kmModel.extractParamMap

val summary = kmModel.summary
summary.trainingCost

// 모델 평가: 계산비용
// -------------------------------------
kmModel.computeCost(predictions)
kmModel.computeCost(predictions.select("features"))

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
        addGrid(km.k, Array(2, 5)).
        addGrid(km.maxIter, Array(10, 20)).
        addGrid(km.initSteps, Array(1, 5)).
        addGrid(km.tol,  Array(0.01, 0.0001)).
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
val cvModel = bestModel.asInstanceOf[KMeansModel]
cvModel.extractParamMap

// 최적모델 저장
// -------------------------------------
cvModel.write.overwrite().save("/DevTools/workspaces/Spark-TDG/chap_13/bestModel")

// 최적모델 불러오기
// -------------------------------------
val loadedModel = KMeansModel.load("/DevTools/workspaces/Spark-TDG/chap_13/bestModel")
