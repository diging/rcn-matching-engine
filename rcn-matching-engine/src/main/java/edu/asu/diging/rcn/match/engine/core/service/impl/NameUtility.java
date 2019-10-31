package edu.asu.diging.rcn.match.engine.core.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

import edu.asu.diging.eaccpf.model.NameEntry;
import edu.asu.diging.eaccpf.model.NamePart;
import edu.asu.diging.rcn.match.engine.core.service.INameUtility;
import edu.asu.diging.rcn.match.engine.core.service.PartType;

@Service
@PropertySource("classpath:/config.properties")
public class NameUtility implements INameUtility {

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

    @Override
    public String getName(NameEntry entry) {
        Map<PartType, List<String>> nameParts = getNameParts(entry);
        if (!nameParts.get(PartType.ORG_NAME).isEmpty()) {
            return String.join(", ", nameParts.get(PartType.ORG_NAME));
        }
        List<String> names = new ArrayList<>();
        names.addAll(nameParts.get(PartType.FIRST_NAME));
        names.addAll(nameParts.get(PartType.LAST_NAME));
        return String.join(" ", names);
    }

    @Override
    public String getPrimayName(NameEntry entry) {
        Map<PartType, List<String>> nameParts = getNameParts(entry);
        if (!nameParts.get(PartType.ORG_NAME).isEmpty()) {
            return String.join(", ", nameParts.get(PartType.ORG_NAME));
        }
        List<String> names = new ArrayList<>();
        names.addAll(nameParts.get(PartType.LAST_NAME));
        return String.join(" ", names);
    }

    @Override
    public String getSecondaryName(NameEntry entry) {
        Map<PartType, List<String>> nameParts = getNameParts(entry);
        List<String> names = new ArrayList<>();
        names.addAll(nameParts.get(PartType.FIRST_NAME));
        return String.join(" ", names);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.asu.diging.rcn.core.service.impl.INameUtility#getNameParts(edu.asu.diging
     * .eaccpf.model.NameEntry)
     */
    @Override
    public Map<PartType, List<String>> getNameParts(NameEntry entry) {
        Map<PartType, List<String>> nameParts = new HashMap<PartType, List<String>>();
        nameParts.put(PartType.FIRST_NAME, new ArrayList<>());
        nameParts.put(PartType.LAST_NAME, new ArrayList<>());
        nameParts.put(PartType.ORG_NAME, new ArrayList<>());
        nameParts.put(PartType.OTHER, new ArrayList<>());
        for (NamePart part : entry.getParts()) {
            String partString = part.getPart();
            List<String> partStringList = Arrays.asList(partString.split(" "));
            if (isFirstName(part)) {
                nameParts.get(PartType.FIRST_NAME).addAll(partStringList);
            } else if (isLastName(part)) {
                nameParts.get(PartType.LAST_NAME).addAll(partStringList);
            } else if (isOrgName(part)) {
                nameParts.get(PartType.ORG_NAME).addAll(partStringList);
            } else {
                nameParts.get(PartType.OTHER).addAll(partStringList);
            }
        }
        return nameParts;
    }

    @Override
    public boolean isLastName(NamePart namePart) {
        return lastNameLocalTypesList.contains(namePart.getLocalType());
    }

    @Override
    public boolean isFirstName(NamePart namePart) {
        return firstNameLocalTypesList.contains(namePart.getLocalType());
    }

    @Override
    public boolean isOrgName(NamePart namePart) {
        return orgNameLocalTypesList.contains(namePart.getLocalType());
    }

    @Override
    public boolean isSameType(NamePart part1, NamePart part2) {
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
