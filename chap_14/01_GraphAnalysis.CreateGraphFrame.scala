// *****************************************************************************
// 프로그램: 01_GraphAnalysis.CreateGraphFrame.scala
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
import org.graphframes.GraphFrame

// *************************************
//  PHASE-1 : 데이터준비
// *************************************
// CSV File -> DataFrame
// -------------------------------------
val bikeStations = spark.read.option("header", "true").
       csv("201508_station_data.csv")

val tripHistory = spark.read.option("header", "true").
       csv("201508_trip_data.csv")

// DataFrame -> Vertices & Edges
// -------------------------------------
val stationVertices = bikeStations.withColumnRenamed("name", "id").
       distinct()

val tripEdges = tripHistory.
       withColumnRenamed("Start Station", "src").
       withColumnRenamed("End Station", "dst")

// *************************************
//  PHASE-2 : 그래프 생성
// *************************************
// GraphFrame
// -------------------------------------
val bikeTripGraph = GraphFrame(stationVertices, tripEdges)
bikeTripGraph.cache()

// 확인
// -------------------------------------
println(s"""
  그래프 정점 개수: ${bikeTripGraph.vertices.count()}
  그래프 간선 개수: ${bikeTripGraph.edges.count()}
원본(station) 개수: ${bikeStations.count()}
   원본(trip) 개수: ${tripHistory.count()}
""")
