package edu.asu.diging.rcn.match.engine.core.kafka;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.asu.diging.rcn.kafka.messages.KafkaTopics;
import edu.asu.diging.rcn.kafka.messages.model.KafkaMatchAuthoritiesJobMessage;
import edu.asu.diging.rcn.match.engine.core.exception.DatasetDoesNotExistException;
import edu.asu.diging.rcn.match.engine.core.service.AuthorityMatcher;

public class MatchDatasetListener {
    
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    @Autowired
    private AuthorityMatcher matcher;
    
    @KafkaListener(topics = KafkaTopics.MATCH_DATASETS_TOPIC)
    public void receiveMessage(String message) {
        ObjectMapper mapper = new ObjectMapper();
        KafkaMatchAuthoritiesJobMessage msg = null;
        try {
            msg = mapper.readValue(message, KafkaMatchAuthoritiesJobMessage.class);
        } catch (IOException e) {
            logger.error("Could not unmarshall message.", e);
            // FIXME: handle this case
            return;
        }
        
        try {
            matcher.process(msg);
        } catch (DatasetDoesNotExistException e) {
            // FIXME: handle this
            logger.error("Could not match dataets.", e);
        }
    }
}