package edu.asu.diging.rcn.match.engine.core.service;

import edu.asu.diging.rcn.kafka.messages.model.KafkaMatchAuthoritiesJobMessage;
import edu.asu.diging.rcn.match.engine.core.exception.DatasetDoesNotExistException;

public interface AuthorityMatcher {

    void process(KafkaMatchAuthoritiesJobMessage msg) throws DatasetDoesNotExistException;

}