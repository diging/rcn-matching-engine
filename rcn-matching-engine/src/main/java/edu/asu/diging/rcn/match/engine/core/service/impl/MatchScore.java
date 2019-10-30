package edu.asu.diging.rcn.match.engine.core.service.impl;

public class MatchScore {

    private float nameScore;
    private float dateScore;
    private float overallScore;

    public float getNameScore() {
        return nameScore;
    }

    public void setNameScore(float nameScore) {
        this.nameScore = nameScore;
    }

    public float getDateScore() {
        return dateScore;
    }

    public void setDateScore(float dateScore) {
        this.dateScore = dateScore;
    }

    public float getOverallScore() {
        return overallScore;
    }

    public void setOverallScore(float overallScore) {
        this.overallScore = overallScore;
    }
}
