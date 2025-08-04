package io.lettuce.core.migration;

import io.lettuce.core.cluster.models.partitions.RedisClusterNode;

public class MigrationCache {
    private static final int SLOT_COUNT = 16384;
    private static final MigrationCache INSTANCE = new MigrationCache();
    private final RedisClusterNode[] migrationTargets = new RedisClusterNode[SLOT_COUNT];

    private MigrationCache() {}

    public static MigrationCache getInstance() {
        return INSTANCE;
    }

    public synchronized void setMigrationTarget(int slot, RedisClusterNode node) {
        migrationTargets[slot] = node;
    }

    public synchronized RedisClusterNode getMigrationTargetBySlot(int slot) {
        return migrationTargets[slot];
    }

    public synchronized boolean isSlotMigrating(int slot) {
        return migrationTargets[slot] != null;
    }

    public synchronized void clearMigrationTarget(int slot) {
        migrationTargets[slot] = null;
    }

    public synchronized void clearAll() {
        for (int i = 0; i < SLOT_COUNT; i++) {
            migrationTargets[i] = null;
        }
    }
} 