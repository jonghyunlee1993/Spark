// *****************************************************************************
// 프로그램: 07_GraphAnalysis.bfs.scala
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
//  PHASE-1 : bfs
// *************************************
// 너비우선탐색
// -------------------------------------
val bfs = bikeTripGraph.bfs.
       fromExpr("id = 'Townsend at 7th'").
       toExpr("id = 'Spear at Folsom'").
       maxPathLength(2).
       run()

// bfs도 데이터프레임이다
// -------------------------------------
bfs.printSchema
// root
//  |-- from: struct (nullable = false)
//  |    |-- station_id: string (nullable = true)
//  |    |-- id: string (nullable = true)
//  |    |-- lat: string (nullable = true)
//  |    |-- long: string (nullable = true)
//  |    |-- dockcount: string (nullable = true)
//  |    |-- landmark: string (nullable = true)
//  |    |-- installation: string (nullable = true)
//  |-- e0: struct (nullable = false)
//  |    |-- Trip ID: string (nullable = true)
//  |    |-- Duration: string (nullable = true)
//  |    |-- Start Date: string (nullable = true)
//  |    |-- src: string (nullable = true)
//  |    |-- Start Terminal: string (nullable = true)
//  |    |-- End Date: string (nullable = true)
//  |    |-- dst: string (nullable = true)
//  |    |-- End Terminal: string (nullable = true)
//  |    |-- Bike #: string (nullable = true)
//  |    |-- Subscriber Type: string (nullable = true)
//  |    |-- Zip Code: string (nullable = true)
//  |-- to: struct (nullable = false)
//  |    |-- station_id: string (nullable = true)
//  |    |-- id: string (nullable = true)
//  |    |-- lat: string (nullable = true)
//  |    |-- long: string (nullable = true)
//  |    |-- dockcount: string (nullable = true)
//  |    |-- landmark: string (nullable = true)
//  |    |-- installation: string (nullable = true)


bfs.show(10)
