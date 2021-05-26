// *****************************************************************************
// 프로그램: 01_MovieRecommender.Ratings.scala
// 작성검증: Barnabas Kim(likebnb@gmail.com)
// 수정일자: 2020-05-23
// 배포버전: 1.0.2
// -----------------------------------------------------------------------------
// 데이터셋: Movie Lens
// 분석주제: 문화.영화.평점
// 분석방법: 추천(Recommendation)
// 컬럼특성: 사용자, 아이템(영화), 평점(레이블), 타임스탬프
// 소스제공: https://grouplens.org/datasets/movielens/
// *****************************************************************************

// *************************************
//  PHASE-1: 데이터준비
// *************************************
// CSV File -> DataFrame
// -------------------------------------
val movieDf = spark.read.format("csv").option("header", true).load("movies.csv")
val ratingDf = spark.read.format("csv").option("header", true).load("ratings.csv")

// Exploring Movie Data
// -------------------------------------
movieDf.printSchema
movieDf.count
movieDf.show(false)

// Exploring Rating Data
// -------------------------------------
ratingDf.printSchema
ratingDf.count
ratingDf.show

// Timestamp to Date
// -------------------------------------
ratingDf.select($"timestamp",
      from_unixtime($"timestamp").alias("rateDate")).show

// 영화별 리뷰수와 평점평균
// -------------------------------------
ratingDf.groupBy("movieId").
      agg(count("movieId").alias("reviewCount"),
          bround(avg("rating"),2).alias("meanRates"),
          min("rating").alias("minRate"),
          max("rating").alias("maxRate")).
      orderBy($"reviewCount".desc).show

// 영화별 리뷰어
// -------------------------------------
ratingDf.groupBy("movieId").
      agg(countDistinct("userId").alias("reviewerCount")).
      orderBy($"reviewerCount".desc).show


// *************************************
//  PHASE-2: CSV to SQL Table
// *************************************

// 1번 이상 리뷰된 영화정보 테이블
// -------------------------------------
spark.sql("DROP TABLE movies")
spark.sql("""
CREATE TABLE movies (
    movieId        Integer     COMMENT '영화아이디',
    title          String      COMMENT '제목',
    genres         String      COMMENT '장르')
USING csv OPTIONS (
    header true,
    path 'movies.csv')
""")

// 영화별 사용자 평점 테이블
// -------------------------------------
spark.sql("DROP TABLE ratings")
spark.sql("""
CREATE TABLE ratings (
    userId         String      COMMENT '사용자아이디',
    movieId        String      COMMENT '영화아이디',
    rating         Double      COMMENT '평점',
    timestamp      Long        COMMENT '타임스탬프')
USING csv OPTIONS (
    header true,
    path 'ratings.csv')
""")

// 테이블 목록 확인
// -------------------------------------
spark.sql("show tables").show

// 타임스탬프 to 날짜포맷
// -------------------------------------
spark.sql("""
SELECT userId, movieId, rating,
       from_unixtime(timestamp) as rateDate
  FROM ratings
""").show

spark.sql("""
SELECT from_unixtime(max(timestamp)) as rateDate
  FROM ratings
""").show

// 평점, 영화정보 테이블 조인
// -------------------------------------
spark.sql("""
SELECT r.userId, r.movieId, r.rating,
       m.title
  FROM ratings as r, movies as m
 WHERE r.movieId = m.movieId
""").show(false)
