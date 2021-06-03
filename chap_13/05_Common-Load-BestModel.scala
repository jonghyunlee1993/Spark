// 최적모델 불러오기
// -------------------------------------
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.evaluation.ClusteringEvaluator

val loadedModel = KMeansModel.
   load("/DevTools/workspaces/Spark-TDG/chap_13/bestModel")

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

val featureCols = Array(
        "checking","duration","history","purpose","amount","savings",
        "employment","instRate","sexMarried","guarantors","regiPeriod",
        "property","age","otherInstPlan","housing","existCredit","job",
        "dependents","ownedPhone","foreignWorker")

val featureAssembler = new VectorAssembler().
        setInputCols(featureCols).
        setOutputCol("features")

val pipeline = new Pipeline().
                setStages(Array(featureAssembler, loadedModel))
val model = pipeline.fit(creditDf)
val predictions = model.transform(creditDf)

val evaluator = new ClusteringEvaluator()
val silhouette = evaluator.evaluate(predictions)
println(s""" 1에 가까운 값이 최적
1st Model's Silhouette: ${silhouette}
""")
