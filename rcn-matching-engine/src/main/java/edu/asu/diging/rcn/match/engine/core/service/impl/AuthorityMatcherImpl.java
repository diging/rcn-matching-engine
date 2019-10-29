package edu.asu.diging.rcn.match.engine.core.service.impl;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.PostConstruct;
import javax.transaction.Transactional;

import org.apache.commons.text.similarity.JaroWinklerDistance;
import org.apache.commons.text.similarity.JaroWinklerSimilarity;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.hibernate.search.jpa.FullTextEntityManager;
import org.hibernate.search.jpa.FullTextQuery;
import org.hibernate.search.jpa.Search;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.stereotype.Service;

import edu.asu.diging.eaccpf.data.DatasetRepository;
import edu.asu.diging.eaccpf.data.NameEntryRepository;
import edu.asu.diging.eaccpf.data.RecordRepository;
import edu.asu.diging.eaccpf.model.Dataset;
import edu.asu.diging.eaccpf.model.NameEntry;
import edu.asu.diging.eaccpf.model.NamePart;
import edu.asu.diging.eaccpf.model.Record;
import edu.asu.diging.eaccpf.model.impl.DatasetImpl;
import edu.asu.diging.eaccpf.model.impl.NameEntryImpl;
import edu.asu.diging.eaccpf.model.impl.RecordImpl;
import edu.asu.diging.eaccpf.model.match.Match;
import edu.asu.diging.eaccpf.model.match.impl.MatchImpl;
import edu.asu.diging.rcn.kafka.messages.model.KafkaMatchAuthoritiesJobMessage;
import edu.asu.diging.rcn.match.engine.core.exception.DatasetDoesNotExistException;
import edu.asu.diging.rcn.match.engine.core.service.AuthorityMatcher;
import edu.asu.diging.rcn.match.engine.core.service.MatchManager;

@Service
@Transactional
@PropertySource("classpath:/config.properties")
public class AuthorityMatcherImpl implements AuthorityMatcher {

    @Autowired
    private DatasetRepository datasetRepository;

    @Autowired
    private NameEntryRepository nameEntryRepo;

    @Autowired
    private RecordRepository recordRepo;

    @Autowired
    private MatchManager matchManager;

    @Autowired
    private JpaTransactionManager transactionManager;

    @Value("${_last_name_local_types}")
    private String lastNameLocalTypes;

    @Value("${_first_name_local_types}")
    private String firstNameLocalTypes;

    @Value("${_org_name_local_types}")
    private String orgNameLocalTypes;

    private List<String> lastNameLocalTypesList;
    private List<String> firstNameLocalTypesList;
    private List<String> orgNameLocalTypesList;

    @PostConstruct
    public void init() {
        lastNameLocalTypesList = Arrays.asList(lastNameLocalTypes.split(","));
        firstNameLocalTypesList = Arrays.asList(firstNameLocalTypes.split(","));
        orgNameLocalTypesList = Arrays.asList(orgNameLocalTypes.split(","));
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.asu.diging.rcn.match.engine.core.service.impl.AuthorityMatcher#process(
     * edu.asu.diging.rcn.kafka.messages.model.KafkaMatchAuthoritiesJobMessage)
     */
    @Override
    public void process(KafkaMatchAuthoritiesJobMessage msg) throws DatasetDoesNotExistException {
        Optional<DatasetImpl> baseOptional = datasetRepository.findById(msg.getBaseDataset());
        if (!baseOptional.isPresent()) {
            throw new DatasetDoesNotExistException("Dataset " + msg.getBaseDataset() + " does not exist.");
        }
        Optional<DatasetImpl> compareOptional = datasetRepository.findById(msg.getMatchDataset());
        if (!compareOptional.isPresent()) {
            throw new DatasetDoesNotExistException("Dataset " + msg.getMatchDataset() + " does not exist.");
        }

        Dataset baseDataset = baseOptional.get();
        Dataset compareDataset = compareOptional.get();

        FullTextEntityManager fullTextEntityManager = Search
                .getFullTextEntityManager(transactionManager.getEntityManagerFactory().createEntityManager());
        QueryBuilder queryBuilder = fullTextEntityManager.getSearchFactory().buildQueryBuilder()
                .forEntity(RecordImpl.class).get();

        recordRepo.getByDataset(baseDataset.getId()).forEach(record -> {
            if (record.getIdentity() != null && record.getIdentity().getNameEntries() != null) {
                record.getIdentity().getNameEntries().forEach(ne -> {
                    List<NamePart> parts = ne.getParts();
                    if (parts != null) {
                        for (NamePart part : parts) {
                            if (isFirstName(part)) {
                                continue;
                            }

                            String name = part.getPart();
                            org.apache.lucene.search.Query query = queryBuilder.keyword().fuzzy()
                                    .onField("identity.nameEntries.parts.part").matching(name).createQuery();

                            FullTextQuery jpaQuery = fullTextEntityManager.createFullTextQuery(query, RecordImpl.class);
                            jpaQuery.setProjection(FullTextQuery.SCORE, FullTextQuery.THIS);
                            List<Object[]> results = jpaQuery.getResultList();

                            for (Object[] searchResult : results) {
                                Record matchedRecord = (Record) searchResult[1];
                                float score = (float) searchResult[0];
                                if (matchedRecord.getDatasetId().equals(compareDataset.getId())) {
                                    for (NameEntry entry : matchedRecord.getIdentity().getNameEntries()) {
                                        for (NamePart part2 : entry.getParts()) {
                                            // this needs to be changed
                                            if (isSameType(part, part2)) {
                                                Match match = new MatchImpl();
                                                match.setLuceneScore(score);
                                                match.setBaseDatasetId(baseDataset.getId());
                                                match.setBaseRecordId(record.getId());
                                                match.setCompareDatasetId(compareDataset.getId());
                                                match.setCompareRecordId(matchedRecord.getId());
                                                match.setOverallScore(scoreMatch(ne, entry, score));
                                                match.setMatchedOn(OffsetDateTime.now());
                                                match.setJobId(msg.getJobId());
                                                match.setInitiator(msg.getInitiator());
                                                matchManager.saveMatch(match);
                                            }
                                        }
                                    }
                                }
                            }

                        }
                    }
                });
            }
        });

    }

    private boolean isLastName(NamePart namePart) {
        return lastNameLocalTypesList.contains(namePart.getLocalType());
    }

    private boolean isFirstName(NamePart namePart) {
        return firstNameLocalTypesList.contains(namePart.getLocalType());
    }

    private boolean isOrgName(NamePart namePart) {
        return orgNameLocalTypesList.contains(namePart.getLocalType());
    }

    private boolean isSameType(NamePart part1, NamePart part2) {
        if (isLastName(part1) && isLastName(part2)) {
            return true;
        }
        if (isFirstName(part1) && isFirstName(part2)) {
            return true;
        }
        if (isOrgName(part1) && isOrgName(part2)) {
            return true;
        }
        return false;
    }

    private float scoreMatch(NameEntry entry1, NameEntry entry2, float luceneScore) {
        float overallScore = luceneScore;
        
        Map<PartType, List<String>> nameParts1 = getNameParts(entry1);
        Map<PartType, List<String>> nameParts2 = getNameParts(entry2);
        
        float lastNameSim = calculateSimilarity(nameParts1.get(PartType.LAST_NAME), nameParts2.get(PartType.LAST_NAME));
        float firstNameSim = calculateSimilarity(nameParts1.get(PartType.FIRST_NAME), nameParts2.get(PartType.FIRST_NAME));
        float orgNameSim = calculateSimilarity(nameParts1.get(PartType.ORG_NAME), nameParts2.get(PartType.ORG_NAME));
        
        if (orgNameSim > -1) {
            return overallScore*orgNameSim;
        }
        
        return overallScore * (lastNameSim * 0.8f) * (firstNameSim * 0.2f);
    }
    
    private float calculateSimilarity(List<String> names1, List<String> names2) {
        JaroWinklerSimilarity jSimilarity = new JaroWinklerSimilarity();
        if (names1.isEmpty() || names2.isEmpty()) {
            return -1;
        }
        float listSim = 0;
        for(String name1 : names1) {
            double sim = -1;
            int idx = 0;
            int matchIdx = 0;
            for (String name2 : names2) {
                double jSim = jSimilarity.apply(name1, name2);
                if (jSim > sim) {
                    sim = jSim;
                    matchIdx = idx;
                }
                idx++;
            }
            listSim += sim;
            if (names2.size() > matchIdx) {
                names2.remove(matchIdx);
            }
        }
        
        return listSim;
    }
    
    private Map<PartType, List<String>> getNameParts(NameEntry entry) {
        Map<PartType, List<String>> nameParts = new HashMap<AuthorityMatcherImpl.PartType, List<String>>();
        nameParts.put(PartType.FIRST_NAME, new ArrayList<>());
        nameParts.put(PartType.LAST_NAME, new ArrayList<>());
        nameParts.put(PartType.ORG_NAME, new ArrayList<>());
        for (NamePart part : entry.getParts()) {
            String partString = part.getPart();
            List<String> partStringList = Arrays.asList(partString.split(" "));
            if (isFirstName(part)) {
                nameParts.get(PartType.FIRST_NAME).addAll(partStringList);
            } else if (isLastName(part)) {
                nameParts.get(PartType.LAST_NAME).addAll(partStringList);
            } else if (isOrgName(part)) {
                nameParts.get(PartType.ORG_NAME).addAll(partStringList);
            }
        }
        return nameParts;
    }
    
    
    enum PartType {
        FIRST_NAME,
        LAST_NAME,
        ORG_NAME;
    }
}
