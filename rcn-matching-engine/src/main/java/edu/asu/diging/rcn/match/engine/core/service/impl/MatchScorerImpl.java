package edu.asu.diging.rcn.match.engine.core.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;

import org.apache.commons.text.similarity.JaroWinklerSimilarity;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

import edu.asu.diging.eaccpf.model.BiogHist;
import edu.asu.diging.eaccpf.model.Date;
import edu.asu.diging.eaccpf.model.DateRange;
import edu.asu.diging.eaccpf.model.Description;
import edu.asu.diging.eaccpf.model.ExistDates;
import edu.asu.diging.eaccpf.model.NameEntry;
import edu.asu.diging.eaccpf.model.NamePart;
import edu.asu.diging.eaccpf.model.Record;
import edu.asu.diging.rcn.match.engine.core.service.MatchScorer;

@Service
@PropertySource("classpath:/config.properties")
public class MatchScorerImpl implements MatchScorer {

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
     * edu.asu.diging.rcn.match.engine.core.service.impl.MatchScorer#score(edu.asu.
     * diging.eaccpf.model.Record, edu.asu.diging.eaccpf.model.Record,
     * edu.asu.diging.eaccpf.model.NameEntry, edu.asu.diging.eaccpf.model.NameEntry,
     * float)
     */
    @Override
    public MatchScore score(Record record1, Record record2, NameEntry entry1, NameEntry entry2, float luceneScore) {
        MatchScore score = new MatchScore();
        score.setNameScore(scoreNameMatch(entry1, entry2, luceneScore));
        score.setDateScore(scoreDatesMatch(record1, record2));
        calculateOverallScore(score);
        return score;
    }

    private float scoreNameMatch(NameEntry entry1, NameEntry entry2, float luceneScore) {
        float overallScore = luceneScore;

        Map<PartType, List<String>> nameParts1 = getNameParts(entry1);
        Map<PartType, List<String>> nameParts2 = getNameParts(entry2);

        float lastNameSim = calculateSimilarity(nameParts1.get(PartType.LAST_NAME), nameParts2.get(PartType.LAST_NAME));
        float firstNameSim = calculateSimilarity(nameParts1.get(PartType.FIRST_NAME),
                nameParts2.get(PartType.FIRST_NAME));
        float orgNameSim = calculateSimilarity(nameParts1.get(PartType.ORG_NAME), nameParts2.get(PartType.ORG_NAME));

        if (orgNameSim > -1) {
            return overallScore * orgNameSim;
        }
        // give a boost if first and last name the same
        if (lastNameSim == 1 && firstNameSim == 1) {
            overallScore = overallScore + 0.2f;
        }
        // if last name is not the same, penalize
        if (lastNameSim < 0.85) {
            overallScore = overallScore - 0.2f;
        }
        return overallScore * (lastNameSim * 0.6f) * (firstNameSim * 0.4f);
    }

    private float scoreDatesMatch(Record record1, Record record2) {
        Description description1 = record1.getDescription();
        Description description2 = record2.getDescription();
        
        if (description1 == null || description2 == null) {
            return -1;
        }
        
        List<BiogHist> bio1 = description1.getBiogHists();
        List<BiogHist> bio2 = description2.getBiogHists();
        ExistDates dates1 = description1.getExistDates();
        ExistDates dates2 = description2.getExistDates();
        
        List<YearRange> ranges1 = new ArrayList<>();
        List<YearRange> ranges2 = new ArrayList<>();
        
        if (dates1 != null) {
            parseYearRanges(dates1.getDateRanges(), ranges1);
            parseDates(dates1.getDates(), ranges1);
        }
        
        if (dates2 != null) {
            parseYearRanges(dates2.getDateRanges(), ranges2);
            parseDates(dates2.getDates(), ranges1);
        }
        
        for(YearRange range1 : ranges1) {
            if (ranges2.isEmpty()) {
                break;
            }
            
            float score = 0f;
            int rangeIdx = 0;
            int matchingIdx = 0;
            for(YearRange range2 : ranges2) {
                float currentScore = 0f;
                if(range1.from <= range2.from+1 && range1.from >= range2.from-1) {
                    // penalize if in range and not exact year
                    currentScore = 0.5f * (1 - Math.abs((range1.from - range2.from)/3));
                }
                if(range1.to <= range2.to+1 && range1.to >= range2.to-1) {
                    // penalize if in range and not exact year
                    currentScore += 0.5f * (1 - Math.abs((range1.to - range2.to)/3));
                }
                
                if (currentScore > score) {
                    score = currentScore;
                    matchingIdx = rangeIdx;
                }
                
                rangeIdx++;
            }
            
            range1.score = score;
            ranges2.remove(matchingIdx);
        }
        
        // average over all years
        float totalScore = 0f;
        int nrOfYearsMatched = 0;
        for(YearRange year : ranges1) {
            totalScore += year.score;
            nrOfYearsMatched++;
        }
        if (nrOfYearsMatched > 0) {
            return totalScore/nrOfYearsMatched;
        }
        return -1;
    }
    
    private void calculateOverallScore(MatchScore score) {
        if (score.getDateScore() == -1 || score.getNameScore() < 0.2) {
            score.setOverallScore(score.getNameScore());
            return;
        }
        
        // we penalize if dates do not match
        if (score.getDateScore() == 0) {
            score.setOverallScore(score.getNameScore() - 0.5f);
            return;
        }
        
        if (score.getDateScore() > 0 && score.getDateScore() < 0.8) {
            score.setOverallScore(score.getNameScore() + 0.05f);
            return;
        }
        
        score.setOverallScore(score.getNameScore()*0.6f + 0.1f);
    }
    
    private void parseYearRanges(List<DateRange> ranges, List<YearRange> years) {
        if (ranges != null && !ranges.isEmpty()) {
            for (DateRange range : ranges) {
                YearRange yearRange = new YearRange();
                Date fromDate = range.getFromDate();
                if (fromDate != null && fromDate.getDate() != null) {
                    yearRange.from = findYear(fromDate.getDate());
                }
                Date toDate = range.getToDate();
                if (toDate != null && toDate.getDate() != null) {
                    yearRange.to = findYear(toDate.getDate());
                }
                years.add(yearRange);
            }
        }
    }
    
    private void parseDates(List<Date> dates, List<YearRange> years) {
        if(dates != null && !dates.isEmpty()) {
            for(Date date : dates) {
                YearRange yearRange = new YearRange();
                String fromDate = date.getNotBefore();
                if (fromDate != null) {
                    yearRange.from = findYear(fromDate);
                }
                String toDate = date.getNotAfter();
                if (toDate != null) {
                    yearRange.to = findYear(toDate);
                }
                years.add(yearRange);
            }
        }
    }
    
    private int findYear(String yearString) {
        Pattern pattern = Pattern.compile("[0-9]{4}");
        Matcher matcher = pattern.matcher(yearString);
        // for now let's take the last one
        // FIXME: is this valid?
        String year = null;
        while(matcher.find()) {
            year = matcher.group();
        }
        
        if (year != null) {
            return new Integer(year);
        }
        return Integer.MAX_VALUE;
    }

    private float calculateSimilarity(List<String> names1, List<String> names2) {
        JaroWinklerSimilarity jSimilarity = new JaroWinklerSimilarity();
        if (names1.isEmpty() || names2.isEmpty()) {
            return -1;
        }
        float listSim = 0;
        float matchedNames = 0;
        for (String name1 : names1) {
            double sim = -1;
            int idx = 0;
            int matchIdx = 0;
            for (String name2 : names2) {
                double jSim = jSimilarity.apply(name1.trim(), name2.trim());
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
            matchedNames++;
            if (names2.isEmpty()) {
                break;
            }
        }

        return listSim / matchedNames;
    }

    private Map<PartType, List<String>> getNameParts(NameEntry entry) {
        Map<PartType, List<String>> nameParts = new HashMap<PartType, List<String>>();
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

    private boolean isLastName(NamePart namePart) {
        return lastNameLocalTypesList.contains(namePart.getLocalType());
    }

    private boolean isFirstName(NamePart namePart) {
        return firstNameLocalTypesList.contains(namePart.getLocalType());
    }

    private boolean isOrgName(NamePart namePart) {
        return orgNameLocalTypesList.contains(namePart.getLocalType());
    }

    enum PartType {
        FIRST_NAME, LAST_NAME, ORG_NAME;
    }
    
    class YearRange {
        public int from;
        public int to;
        public float score;
    }
}
