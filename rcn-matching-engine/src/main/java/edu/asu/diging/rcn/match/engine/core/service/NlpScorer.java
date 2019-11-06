package edu.asu.diging.rcn.match.engine.core.service;

public interface NlpScorer {

    float calculateKeywordSimilarity(String text1, String text2);

}