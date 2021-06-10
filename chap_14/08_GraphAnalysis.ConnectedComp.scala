// *****************************************************************************
// 프로그램: 08_GraphAnalysis.ConnectedComp.scala
// 작성검증: Barnabas Kim(likebnb@gmail.com)
// 수정일자: 2020-05-23
// 배포버전: 1.0.2
// -----------------------------------------------------------------------------
// 데이터셋: Bike bikeTripGraph
// 분석주제: 교통.라스트마일
// 분석방법: 그래프(GraphFrame)
// 컬럼특성: 정점(id), 간선(src, dst)
// 소스제공: 교재
// *****************************************************************************
//:load 01_GraphAnalysis.CreateGraphFrame.scala

// *************************************
//  PHASE-1 : 데이터준비
// *************************************
//  연결요소 찾기 작업은 체크포인트 필요
// -------------------------------------
spark.sparkContext.setCheckpointDir("C:/DevTools/workspaces/temp")

// 작은 그래프로 시작하기(1/10 크기 샘플)
// -------------------------------------
val sampleGraph = GraphFrame(stationVertices,
       tripEdges.sample(false, 0.1))

// 연결요소 찾기
// -------------------------------------
val cc = sampleGraph.connectedComponents.run()
cc.printSchema
cc.show(10, false)

// 고립된 정점이 있었다는 것을 기억하자
// -------------------------------------
cc.select("component").distinct().show(10, false)
cc.groupBy("component").count().show


// 강한연결요소 찾기
// -------------------------------------
val scc = sampleGraph.stronglyConnectedComponents.
        maxIter(20).
        run()

scc.printSchema
scc.show(10, false)

// 두 개의 강한연결요소(부분그래프) 발견
// -------------------------------------
scc.select("component").distinct().show(10, false)
scc.groupBy("component").count().show
