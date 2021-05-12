// *****************************************************************************
// 프로그램: 01_classification.LogisticRegression.scala
// 작성검증: Barnabas Kim(likebnb@gmail.com)
// 수정일자: 2020-05-23
// 배포버전: 1.0.2
// -----------------------------------------------------------------------------
// 모델유형: 분류(Classification)
// 알고리즘: 로지스틱회귀(LogisticRegression)
// 프로세스: 소스 > 특징변환 > 모델정의 > 파이프라인(학습,예측,평가) > 모델선택
// 모델평가: accuracy, bestThreshold
// *****************************************************************************
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

// *************************************
//  PHASE-1: 데이터준비
// *************************************
// Text File -> DataFrame
// -------------------------------------
val data = spark.read.format("libsvm").load("sample_libsvm_data.txt")
data.printSchema
data.select("label").distinct.show
data.select("features").show(false)
data.count


// *************************************
//  PHASE-2: 데이터변환(transformer)
// *************************************
// 2-1: StringIndexer
// -------------------------------------
val labelIndexer = new StringIndexer().
                 setInputCol("label").
                 setOutputCol("indexedLabel").
                 fit(data)

// 2-2: VectorIndexer
// -------------------------------------
val featureIndexer = new VectorIndexer().
                 setInputCol("features").
                 setOutputCol("indexedFeatures").
                 setMaxCategories(4).
                 fit(data)


// *************************************
//  PHASE-3: 모델정의
// *************************************
// 3-1: 데이터셋 분할(학습,검증)
// -------------------------------------
val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

// 3-2: 로지스틱회귀 모델 정의
// -------------------------------------
val lr = new LogisticRegression().
                 setMaxIter(10).
                 setRegParam(0.3).
                 setElasticNetParam(0.8).
                 setLabelCol("indexedLabel").
                 setFeaturesCol("indexedFeatures").
                 setThreshold(0.2)
println(lr.explainParams())

// 3-3: 변환기(인덱스 -> 문자열)
// -------------------------------------
val labelConverter = new IndexToString().
                 setInputCol("prediction").
                 setOutputCol("predictedLabel").
                 setLabels(labelIndexer.labels)


// *************************************
//  PHASE-4: 파이프라인
// *************************************
// 4-1: 파이프라인 정의
// -------------------------------------
val pipeline = new Pipeline().
                 setStages(Array(
                     labelIndexer,
                     featureIndexer,
                     lr,
                     labelConverter
                 ))

// 4-2: 파이프라인 학습
// -------------------------------------
val model = pipeline.fit(trainingData)

// 4-3: 파이프라인 실행(예측)
// -------------------------------------
val predictions = model.transform(testData)
predictions.select("predictedLabel", "label", "features").show(5)


// *************************************
//  PHASE-5: 파이프라인 평가
// *************************************
// 5-1: 평가기 정의
// -------------------------------------
val evaluator = new MulticlassClassificationEvaluator().
                 setLabelCol("indexedLabel").
                 setPredictionCol("prediction").
                 setMetricName("accuracy")

// 5-2: 평가메트릭(accuracy)
// -------------------------------------
val accuracy = evaluator.evaluate(predictions)
println(s"Test Error = ${(1.0 - accuracy)}")

// 5-3: 모델 요약
// -------------------------------------
val lrModel = model.stages(2).asInstanceOf[LogisticRegressionModel]
println(s"Coefficients:\n ${lrModel.coefficients}")
println(s"Intercept:\n ${lrModel.intercept}")

val trainSummary = lrModel.binarySummary
println(trainSummary.areaUnderROC)
trainSummary.roc.show()
trainSummary.pr.show()


// *************************************
//  PHASE-6: 파이프라인 성능
// *************************************
// 6-1: Objective History
// -------------------------------------
val objSummary = trainSummary.objectiveHistory
println("objSummary:")
objSummary.foreach(loss => println(loss))

// 6-2: 최적 문턱값 찾기
// -------------------------------------
val fMeasure = trainSummary.fMeasureByThreshold
val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure).
                    select("threshold").head().getDouble(0)
println(bestThreshold)


// *************************************
//  PHASE-7: 최적값 적용한 모델
// *************************************
// 7-1: 모델(bestThreshold)
// -------------------------------------
val lr2 = new LogisticRegression().
                 setMaxIter(10).
                 setRegParam(0.3).
                 setElasticNetParam(0.8).
                 setLabelCol("indexedLabel").
                 setFeaturesCol("indexedFeatures").
                 setThreshold(bestThreshold)

// 7-2: 파이프라인 정의
// -------------------------------------
val pipeline2 = new Pipeline().
                 setStages(Array(labelIndexer, featureIndexer, lr2, labelConverter))

// 7-2: 파이프라인 학습
// -------------------------------------
val model2 = pipeline2.fit(trainingData)

// 7-3: 파이프라인 실행(예측)
// -------------------------------------
val predictions2 = model2.transform(testData)

// 7-4: 파이프라인 평가(accuracy)
// -------------------------------------
val evaluator2 = new MulticlassClassificationEvaluator().
                 setLabelCol("indexedLabel").
                 setPredictionCol("prediction").
                 setMetricName("accuracy")
val accuracy2 = evaluator2.evaluate(predictions2)
println(s"""Test1 Error = ${(1.0 - accuracy)} \tAccuray = ${accuracy} \tThreshold = ${lr.getThreshold}
Test2 Error = ${(1.0 - accuracy2)} \tAccuray = ${accuracy2} \tThreshold = ${lr2.getThreshold}\n""")
