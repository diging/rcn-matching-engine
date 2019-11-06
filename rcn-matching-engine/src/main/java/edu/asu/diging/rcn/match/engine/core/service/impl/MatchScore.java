package edu.asu.diging.rcn.match.engine.core.service.impl;

public class MatchScore {

    private float nameScore;
    private float dateScore;
    private float overallScore;
    private float bioScore;

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

    public float getBioScore() {
        return bioScore;
    }

    public void setBioScore(float bioScore) {
        this.bioScore = bioScore;
    }
}
