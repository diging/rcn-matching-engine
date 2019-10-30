package edu.asu.diging.rcn.match.engine.core.service.impl;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import javax.annotation.PostConstruct;
import javax.transaction.Transactional;

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
import edu.asu.diging.eaccpf.data.RecordRepository;
import edu.asu.diging.eaccpf.model.Dataset;
import edu.asu.diging.eaccpf.model.NameEntry;
import edu.asu.diging.eaccpf.model.NamePart;
import edu.asu.diging.eaccpf.model.Record;
import edu.asu.diging.eaccpf.model.impl.DatasetImpl;
import edu.asu.diging.eaccpf.model.impl.RecordImpl;
import edu.asu.diging.eaccpf.model.match.Match;
import edu.asu.diging.eaccpf.model.match.impl.MatchImpl;
import edu.asu.diging.rcn.kafka.messages.model.KafkaMatchAuthoritiesJobMessage;
import edu.asu.diging.rcn.match.engine.core.exception.DatasetDoesNotExistException;
import edu.asu.diging.rcn.match.engine.core.service.AuthorityMatcher;
import edu.asu.diging.rcn.match.engine.core.service.MatchManager;
import edu.asu.diging.rcn.match.engine.core.service.MatchScorer;

@Service
@Transactional
@PropertySource("classpath:/config.properties")
public class AuthorityMatcherImpl implements AuthorityMatcher {

    @Autowired
    private DatasetRepository datasetRepository;

    @Autowired
    private RecordRepository recordRepo;

    @Autowired
    private MatchManager matchManager;

    @Autowired
    private JpaTransactionManager transactionManager;
    
    @Autowired
    private MatchScorer scorer;

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
                                                match.setMatchedOn(OffsetDateTime.now());
                                                match.setJobId(msg.getJobId());
                                                match.setInitiator(msg.getInitiator());
                                                
                                                MatchScore matchScore = scorer.score(record, matchedRecord, ne, entry, score);
                                                match.setNameScore(matchScore.getNameScore());
                                                match.setDateScore(matchScore.getDateScore());
                                                match.setOverallScore(matchScore.getOverallScore());
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

    
}
