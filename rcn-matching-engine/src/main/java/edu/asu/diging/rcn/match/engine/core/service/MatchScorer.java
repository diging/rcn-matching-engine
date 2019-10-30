package edu.asu.diging.rcn.match.engine.core.service;

import edu.asu.diging.eaccpf.model.NameEntry;
import edu.asu.diging.eaccpf.model.Record;
import edu.asu.diging.rcn.match.engine.core.service.impl.MatchScore;

public interface MatchScorer {

    MatchScore score(Record record1, Record record2, NameEntry entry1, NameEntry entry2, float luceneScore);

}