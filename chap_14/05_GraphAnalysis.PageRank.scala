// *****************************************************************************
// 프로그램: 05_GraphAnalysis.PageRank.scala
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
//  PHASE-1 : PageRank
// *************************************

// PageRank 실행
// -------------------------------------
val ranks = bikeTripGraph.
       pageRank.
       resetProbability(0.15).
       maxIter(10).
       run()

ranks.vertices.printSchema
// root
//  |-- station_id: string (nullable = true)
//  |-- id: string (nullable = true)
//  |-- lat: string (nullable = true)
//  |-- long: string (nullable = true)
//  |-- dockcount: string (nullable = true)
//  |-- landmark: string (nullable = true)
//  |-- installation: string (nullable = true)
//  |-- pagerank: double (nullable = true)

// 결과도 그래프프레임
// -------------------------------------
ranks.vertices.select(
       $"id", bround(col("pagerank"),2).as("pageRank"),
       $"landmark").
       orderBy(desc("pagerank")).
       show(10, false

// 칼트레인역이 높은 순위에 있는 이유에 대해 생각해보자
// 대부분의 자전거 여행이 끝나는 지리적 연결지점이자
// 출근하는 직장인들의 Last Mile이기 때문이다
// +----------------------------------------+--------+-------------+
// |id                                      |pageRank|landmark     |
// +----------------------------------------+--------+-------------+
// |San Jose Diridon Caltrain Station       |4.05    |San Jose     |
// |San Francisco Caltrain (Townsend at 4th)|3.35    |San Francisco|
// |Mountain View Caltrain Station          |2.51    |Mountain View|
// |Redwood City Caltrain Station           |2.33    |Redwood City |
// |San Francisco Caltrain 2 (330 Townsend) |2.23    |San Francisco|
// |Harry Bridges Plaza (Ferry Building)    |1.83    |San Francisco|
// |2nd at Townsend                         |1.58    |San Francisco|
// |Santa Clara at Almaden                  |1.57    |San Jose     |
// |Townsend at 7th                         |1.57    |San Francisco|
// |Embarcadero at Sansome                  |1.54    |San Francisco|
// +----------------------------------------+--------+-------------+
