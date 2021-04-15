package org.likebnb.ds.spark.analyzer

import org.apache.spark.sql.Dataset
import org.bitbucket.eunjeon.seunjeon.Analyzer

class KoreanAnalyzer extends Serializable {}

object KoreanAnalyzer {
    val colNames = Seq("term", "pos")
  
    def analyze(line: String) = {
        val terms = Analyzer.parse(line).flatMap(_.deCompound())
                    .map(node => (node.morpheme.surface, 
                                  node.morpheme.poses.apply(0).toString()))
        terms
    }
    
    def filterPos(terms: Dataset[(String, String)], pos: String) = {
        val words = terms.filter(_._2.equals(pos)).toDF(colNames: _*)
        words
    }
}