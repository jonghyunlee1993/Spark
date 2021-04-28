// -- 4장의 CSV 파일을 읽어서 확인
//:paste ..\\chap_04\\03_spark.read.schema.option.scala
:paste ../chap_04/03_spark.read.schema.option.scala
df.printSchema
df.show

// -- 만약 테이블이 이미 존재할 경우 재생성하려면
spark.sql("DROP TABLE AiRoBiC")

// -- 테이블 생성
spark.sql("""
  CREATE TABLE AiRoBiC (
     bicNumber      String  COMMENT '자전거번호',
     rentDate       Date    COMMENT '대여일자',
     rentStatId     String  COMMENT '대여소번호',
     rentStatName   String  COMMENT '대여소이름',
     rentParkId     String  COMMENT '거치대번호',
     returnDate     Date    COMMENT '반납일자',
     returnStatId   String  COMMENT '반납대여소번호',
     returnStatName String  COMMENT '반납대여소이름',
     returnParkId   String  COMMENT '반납거치대번호',
     useTime        Long    COMMENT '사용시간(분)',
     useDistance    Long    COMMENT '사용거리(미터)')
  USING csv OPTIONS (
     header true,
--     path 'C:\\DevTools\\workspaces\\Spark-TDG\\chap_05\\AiRoBiC_2015_0919_ALL.csv')
     path '/DevTools/workspaces/Spark-TDG/chap_05/AiRoBiC_2015_0919_ALL.csv')
""")
spark.sql("SHOW TABLES").show
spark.sql("DESCRIBE AIRoBiC").show
spark.sql("DESC AIRoBiC").show

// -- :quit로 세션 종료 및 재시작 후 쿼리 실행
spark.sql("SELECT * FROM AiRoBiC").show
