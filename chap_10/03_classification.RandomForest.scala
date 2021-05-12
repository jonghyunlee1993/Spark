// *****************************************************************************
// 프로그램: 03_classification.RandomForest.scala
// 작성검증: Barnabas Kim(likebnb@gmail.com)
// 수정일자: 2020-05-23
// 배포버전: 1.0.2
// -----------------------------------------------------------------------------
// 모델유형: 분류(Classification)
// 알고리즘: 랜덤포레스트(RandomForest)
// 프로세스: 소스 > 특징변환 > 모델정의 > 파이프라인(학습,예측,평가) > 모델선택
// 모델평가: accuracy, toDebugString
// *****************************************************************************
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}

// *************************************
//  PHASE-1: 데이터준비
// *************************************
// Text File -> DataFrame
// -------------------------------------
val data = spark.read.format("libsvm").load("sample_libsvm_data.txt")


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

// 3-2: 랜덤포레스트 모델 정의
// -------------------------------------
val rf = new RandomForestClassifier().
          setLabelCol("indexedLabel").
          setFeaturesCol("indexedFeatures").
          setNumTrees(2)

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
              rf,
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
val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
println(s"Learned random forest model:\n ${rfModel.toDebugString}")
