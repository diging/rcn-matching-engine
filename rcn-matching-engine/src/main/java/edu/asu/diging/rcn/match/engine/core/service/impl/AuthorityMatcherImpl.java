package edu.asu.diging.rcn.match.engine.core.service.impl;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.transaction.Transactional;

import org.hibernate.search.jpa.FullTextEntityManager;
import org.hibernate.search.jpa.FullTextQuery;
import org.hibernate.search.jpa.Search;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.stereotype.Service;

import edu.asu.diging.eaccpf.data.DatasetRepository;
import edu.asu.diging.eaccpf.data.MasterMatchRepository;
import edu.asu.diging.eaccpf.data.RecordRepository;
import edu.asu.diging.eaccpf.model.Dataset;
import edu.asu.diging.eaccpf.model.NameEntry;
import edu.asu.diging.eaccpf.model.NamePart;
import edu.asu.diging.eaccpf.model.Record;
import edu.asu.diging.eaccpf.model.impl.DatasetImpl;
import edu.asu.diging.eaccpf.model.impl.RecordImpl;
import edu.asu.diging.eaccpf.model.match.MasterMatch;
import edu.asu.diging.eaccpf.model.match.Match;
import edu.asu.diging.eaccpf.model.match.impl.MasterMatchImpl;
import edu.asu.diging.eaccpf.model.match.impl.MatchImpl;
import edu.asu.diging.rcn.kafka.messages.model.KafkaMatchAuthoritiesJobMessage;
import edu.asu.diging.rcn.match.engine.core.exception.DatasetDoesNotExistException;
import edu.asu.diging.rcn.match.engine.core.service.AuthorityMatcher;
import edu.asu.diging.rcn.match.engine.core.service.INameUtility;
import edu.asu.diging.rcn.match.engine.core.service.MatchManager;
import edu.asu.diging.rcn.match.engine.core.service.MatchScorer;

@Service
@Transactional
@PropertySource("classpath:/config.properties")
public class AuthorityMatcherImpl implements AuthorityMatcher {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private DatasetRepository datasetRepository;

    @Autowired
    private RecordRepository recordRepo;

    @Autowired
    private MatchManager matchManager;

    @Autowired
    private JpaTransactionManager transactionManager;

    @Autowired
    private MasterMatchRepository masterMatchRepo;

    @Autowired
    private MatchScorer scorer;

    @Autowired
    private INameUtility nameUtility;

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
            logger.debug("Matching " + record.getId());

            if (record.getIdentity() != null && record.getIdentity().getNameEntries() != null) {
                record.getIdentity().getNameEntries().forEach(ne -> {
                    if (ne.getScriptCode() == null || ne.getScriptCode().trim().isEmpty()
                            || ne.getScriptCode().trim().toLowerCase().equals("latn")) {
                        List<NamePart> parts = ne.getParts();
                        if (parts != null) {
                            for (NamePart part : parts) {
                                if (nameUtility.isFirstName(part)) {
                                    continue;
                                }

                                String name = part.getPart();
                                org.apache.lucene.search.Query query = queryBuilder.keyword().fuzzy()
                                        .onField("identity.nameEntries.parts.part").matching(name).createQuery();

                                FullTextQuery jpaQuery = fullTextEntityManager.createFullTextQuery(query,
                                        RecordImpl.class);
                                jpaQuery.setProjection(FullTextQuery.SCORE, FullTextQuery.THIS);
                                List<Object[]> results = jpaQuery.getResultList();

                                for (Object[] searchResult : results) {
                                    Record matchedRecord = (Record) searchResult[1];
                                    float score = (float) searchResult[0];
                                    if (matchedRecord.getDatasetId().equals(compareDataset.getId())) {
                                        for (NameEntry entry : matchedRecord.getIdentity().getNameEntries()) {
                                            for (NamePart part2 : entry.getParts()) {
                                                // this needs to be changed
                                                if (nameUtility.isSameType(part, part2)) {

                                                    MatchScore matchScore = scorer.score(record, matchedRecord, ne,
                                                            entry, score);
                                                    if (matchScore.getOverallScore() > 0.1) {
                                                        Match match = new MatchImpl();
                                                        match.setLuceneScore(score);
                                                        match.setBaseDatasetId(baseDataset.getId());
                                                        match.setBaseRecordId(record.getId());
                                                        match.setCompareDatasetId(compareDataset.getId());
                                                        match.setCompareRecordId(matchedRecord.getId());
                                                        match.setMatchedOn(OffsetDateTime.now());
                                                        match.setJobId(msg.getJobId());
                                                        match.setInitiator(msg.getInitiator());

                                                        match.setNameScore(matchScore.getNameScore());
                                                        match.setDateScore(matchScore.getDateScore());
                                                        match.setBioScore(matchScore.getBioScore());
                                                        match.setOverallScore(matchScore.getOverallScore());
                                                        matchManager.saveMatch(match);

                                                        MasterMatch master = masterMatchRepo
                                                                .findFirstByJobIdAndRecordId(msg.getJobId(),
                                                                        record.getId());
                                                        if (master == null) {
                                                            master = new MasterMatchImpl();
                                                            master.setJobId(msg.getJobId());
                                                            master.setDatasetId(baseDataset.getId());
                                                            master.setRecordId(record.getId());
                                                            master.setMatchedDatasetId(compareDataset.getId());
                                                            master.setMatchedRecordId(matchedRecord.getId());
                                                            master.setMatches(new ArrayList<Match>());
                                                        }
                                                        if (master.getScore() < match.getOverallScore()) {
                                                            master.setNamePart1(nameUtility.getPrimayName(ne));
                                                            master.setNamePart2(nameUtility.getSecondaryName(ne));
                                                            master.setScore(match.getOverallScore());
                                                            master.setMaster(match);
                                                        }
                                                        master.getMatches().add(match);
                                                        masterMatchRepo.save((MasterMatchImpl) master);
                                                    }
                                                }
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

        logger.info("Done matching authorities.");
    }

}
