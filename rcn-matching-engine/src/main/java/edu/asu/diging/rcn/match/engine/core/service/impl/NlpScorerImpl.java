package edu.asu.diging.rcn.match.engine.core.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Service;

import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.asu.diging.rcn.match.engine.core.service.NlpScorer;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import info.debatty.java.stringsimilarity.LongestCommonSubsequence;

@Service
public class NlpScorerImpl implements NlpScorer {
    
    private StanfordCoreNLP pipeline;
    
    @PostConstruct
    public void init() {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner");
        pipeline = new StanfordCoreNLP(props);
    }

    /* (non-Javadoc)
     * @see edu.asu.diging.rcn.match.engine.core.service.impl.NlpScorer#calculateKeywordSimilarity(java.lang.String, java.lang.String)
     */
    @Override
    public float calculateKeywordSimilarity(String text1, String text2) {
        
        List<String> significantWords = getWordVector(text1);
        List<String> significantWords2 = getWordVector(text2);
        Collections.sort(significantWords);
        Collections.sort(significantWords2);

        String sigWords1 = String.join(" ", significantWords);
        String sigWords2 = String.join(" ", significantWords2);

        LongestCommonSubsequence lcs = new LongestCommonSubsequence();
        double lcsDist = lcs.distance(sigWords1, sigWords2);
        // calculate difference in length
        double diff = Math.abs(sigWords1.length() - sigWords2.length());
        // remove all insertion necessary to account for length
        lcsDist = lcsDist - diff;
        // normalize by longest text
        return sigWords1.length() > sigWords2.length() ? (float)lcsDist/(float)sigWords1.length() : (float)lcsDist/(float)sigWords2.length();
    }
    
    private List<String> getWordVector(String text) {
        CoreDocument document = new CoreDocument(text);

        // run all Annotators on this text
        pipeline.annotate(document);

        List<String> significantWords = new ArrayList<>();
        List<String> validPos = Arrays.asList("NN", "NNS", "NNP");
        List<String> validEntities = Arrays.asList("TITLE", "NATIONALITY", "LOCATION", "PERSON", "DATE", "ORGANIZATION",
                "MISC", "TIME", "DURATION", "SET", "STATE_OR_PROVINCE", "COUNTRY", "RELIGION", "IDEOLOGY",
                "CRIMINAL_CHARGE", "CAUSE_OF_DEATH");
        for (CoreLabel token : document.tokens()) {
            String pos = token.get(PartOfSpeechAnnotation.class);
            String ne = token.get(NamedEntityTagAnnotation.class);
            if ((pos != null && validPos.contains(pos)) || (ne != null && validEntities.contains(ne))) {
                significantWords.add(token.get(TextAnnotation.class));
            }
        }
        return significantWords;
    }
}
