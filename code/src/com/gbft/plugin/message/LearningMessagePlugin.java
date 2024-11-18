package com.gbft.plugin.message;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import com.gbft.framework.core.Entity;
import com.gbft.framework.data.MessageData;
import com.gbft.framework.plugins.MessagePlugin;
import com.gbft.framework.utils.DataUtils;
import com.gbft.framework.utils.MessageTally;
import com.gbft.framework.utils.MessageTally.QuorumId;
import org.apache.commons.lang3.tuple.Pair;

public class LearningMessagePlugin implements MessagePlugin {

    private Entity entity;
    private MessageTally tally;

    public LearningMessagePlugin(Entity entity) {
        this.entity = entity;
        tally = entity.getReportTally();
    }

    @Override
    public MessageData processIncomingMessage(MessageData message) {
        if (entity.isClient() || message.getMessageType() != entity.REPORT) {
            return message;
        }

        var seqnum = message.getSequenceNum();
        var source = message.getSource();
        var report = message.getReport().getReportMap();
        entity.getReports().computeIfAbsent((int)seqnum, e -> new ConcurrentHashMap<>())
                .computeIfAbsent(source, s -> new ConcurrentHashMap<>())
                .putAll(report);
                
        // collects report messages into a report quorum
        tally.tally(message);

        if (entity.getReportTally().hasQuorum(Pair.of(entity.getId()/4L, seqnum), 0, new QuorumId(entity.REPORT, entity.REPORT_QUORUM))) {
            entity.stateUpdateLoop(Pair.of(entity.getId()/4L, entity.exchangeSequence));
        }

        return DataUtils.invalidate(message);
    }

    @Override
    public MessageData processOutgoingMessage(MessageData message) {
        return message;
    }

}
