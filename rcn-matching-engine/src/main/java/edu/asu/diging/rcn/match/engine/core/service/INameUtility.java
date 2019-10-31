package edu.asu.diging.rcn.match.engine.core.service;

import java.util.List;
import java.util.Map;

import edu.asu.diging.eaccpf.model.NameEntry;
import edu.asu.diging.eaccpf.model.NamePart;

public interface INameUtility {

    Map<PartType, List<String>> getNameParts(NameEntry entry);

    String getName(NameEntry entry);

    boolean isSameType(NamePart part1, NamePart part2);

    boolean isOrgName(NamePart namePart);

    boolean isFirstName(NamePart namePart);

    boolean isLastName(NamePart namePart);

    String getSecondaryName(NameEntry entry);

    String getPrimayName(NameEntry entry);

}