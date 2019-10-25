package edu.asu.diging.rcn.match.engine.core.service.impl;

import javax.transaction.Transactional;
import javax.transaction.Transactional.TxType;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import edu.asu.diging.eaccpf.data.MatchRepository;
import edu.asu.diging.eaccpf.model.match.Match;
import edu.asu.diging.eaccpf.model.match.impl.MatchImpl;
import edu.asu.diging.rcn.match.engine.core.service.MatchManager;

@Service
public class MatchManagerImpl implements MatchManager {

    @Autowired
    private MatchRepository matchRepo;
    
    /* (non-Javadoc)
     * @see edu.asu.diging.rcn.match.engine.core.service.impl.MatchManager#saveMatch(edu.asu.diging.eaccpf.model.match.Match)
     */
    @Override
    @Transactional(value=TxType.REQUIRES_NEW)
    public void saveMatch(Match match) {
        matchRepo.save((MatchImpl) match);
    }
}
