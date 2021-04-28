// -----------------------------------------------------------------------------
// JOIN 실습을 위한 데이터셋 생성
// -----------------------------------------------------------------------------
val student = Seq(
      (1, "박계산", "전자계산학과"),
      (2, "신기록", "산업공학과"),
      (3, "문교부", "산업공학과"),
      (5, "이기자", "전자계산학과")
    ).toDF("studentId", "name", "department")
val courseReg = Seq(
      (1, 101, "Spark와 기계학습", "전자계산학과"),
      (2, 101, "Spark와 기계학습", "전자계산학과"),
      (3, 201, "Optimization", "산업공학과"),
      (4, 201, "Optimization", "산업공학과")
    ).toDF("studentId", "courseId", "courseName", "department")
// -----------------------------------------------------------------------------
student.createOrReplaceTempView("student")
courseReg.createOrReplaceTempView("courseReg")
// -----------------------------------------------------------------------------
spark.sql("show tables").show
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// JOIN을 위한 표현식과 조인실행
// -----------------------------------------------------------------------------
val joinExpression = student.col("studentId") === courseReg.col("studentId")
student.join(courseReg, joinExpression).show()

// -----------------------------------------------------------------------------
// INNER JOIN
// -----------------------------------------------------------------------------
var joinType = "inner"
student.join(courseReg, joinExpression, joinType).show()

// -----------------------------------------------------------------------------
// OUTER JOIN
// -----------------------------------------------------------------------------
joinType = "outer"
student.join(courseReg, joinExpression, joinType).show()

// -----------------------------------------------------------------------------
// LEFT OUTER JOIN
// -----------------------------------------------------------------------------
joinType = "left_outer"
student.join(courseReg, joinExpression, joinType).show()

// -----------------------------------------------------------------------------
// RIGHT OUTER JOIN
// -----------------------------------------------------------------------------
joinType = "right_outer"
student.join(courseReg, joinExpression, joinType).show()

// -----------------------------------------------------------------------------
// FULL OUTER JOIN
// -----------------------------------------------------------------------------
joinType = "full_outer"
student.join(courseReg, joinExpression, joinType).show()

// -----------------------------------------------------------------------------
// LEFT SEMI JOIN
// -----------------------------------------------------------------------------
joinType = "left_semi"
student.join(courseReg, joinExpression, joinType).show()

// -----------------------------------------------------------------------------
// LEFT ANTI JOIN
// -----------------------------------------------------------------------------
joinType = "left_anti"
student.join(courseReg, joinExpression, joinType).show()

// -----------------------------------------------------------------------------
// CROSS JOIN
// -----------------------------------------------------------------------------
joinType = "cross"
student.join(courseReg, joinExpression, joinType).show() // 내부조인과 같음
student.crossJoin(courseReg).show() // 카티시안 조인
