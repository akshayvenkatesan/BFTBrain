package com.gbft.plugin.role;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.gbft.framework.core.Entity;
import com.gbft.framework.plugins.RolePlugin;
import com.gbft.framework.statemachine.StateMachine;
import com.gbft.framework.utils.Config;
import com.gbft.framework.utils.EntityMapUtils;

public class PrimaryPassivePlugin extends RolePlugin {

    int f;

    final int PRIMARY;
    public final int ACTIVE;
    public final int PASSIVE;

    public PrimaryPassivePlugin(Entity entity) {
        super(entity);

        f = Config.integer("general.f");

        PRIMARY = StateMachine.roles.indexOf("primary");
        ACTIVE = StateMachine.roles.indexOf("active");
        PASSIVE = StateMachine.roles.indexOf("passive");
    }

    @Override
    protected List<Integer> getRoleEntities(long offset, int phase, int role, int clusternum) {
        return null;
    }

    @Override
    protected List<Integer> getRoleEntities(long offset, int phase, int role) {
        /*
         * This method returns the list of entities that match a role
         * based on the offset, phase and role passed as arguments.
         * offset: the offset to be used to calculate the index of the entity
         * phase: normal phase or recovery phase
         * role: the role of the entity
         */
        if (role == StateMachine.CLIENT) {
            return EntityMapUtils.getAllClients();
        }

        var total = EntityMapUtils.nodeCount();
        if (phase != StateMachine.NORMAL_PHASE) {
            offset -= 1;
        }

        var base = (int) ((offset + total) % total);

        if (role == PRIMARY) {
            var index = phase == StateMachine.NORMAL_PHASE ? base : base + 1;
            var entity = EntityMapUtils.getNodeId(index % total);
            return List.of(entity);
        } else if (role == ACTIVE) {
            return IntStream.range(base, base + total - f)
                    .map(index -> EntityMapUtils.getNodeId(index % total)).boxed()
                    .collect(Collectors.toList());
        } else if (role == PASSIVE) {
            return IntStream.range(base + total - f, base + total)
                    .map(index -> EntityMapUtils.getNodeId(index % total)).boxed()
                    .collect(Collectors.toList());
        } else if (role == StateMachine.NODE) {
            return EntityMapUtils.getAllNodes();
        }

        return null;
    }

    @Override
    protected List<Integer> getEntityRoles(long offset, int phase, int entity) {
        /*
         * Finds index of the id passed as entity in the list of nodes.
         * If the entity is not found, returns a list containing the CLIENT
         * because client does not exist
         */
        var index = EntityMapUtils.getNodeIndex(entity);
        if (index < 0) {
            return List.of(StateMachine.CLIENT);
        }

        var total = EntityMapUtils.nodeCount();
        if (phase != StateMachine.NORMAL_PHASE) {
            offset -= 1;
        }

        var point = (index - (offset % total) + total) % total;
        if (point == (phase == StateMachine.NORMAL_PHASE ? 0 : 1)) {
            return List.of(PRIMARY, ACTIVE, StateMachine.NODE);
        } else if (point < total - f) {
            return List.of(ACTIVE, StateMachine.NODE);
        } else {
            return List.of(PASSIVE, StateMachine.NODE);
        }

    }
}
