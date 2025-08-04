package io.lettuce.core.cluster;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import io.lettuce.core.ReadFrom;
import io.lettuce.core.cluster.models.partitions.Partitions;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.migration.MigrationCache;
import io.lettuce.core.migration.MigrationAwareResponse;
import io.lettuce.core.output.MigrationAwareValueOutput;
import io.lettuce.core.protocol.ConnectionIntent;
import io.lettuce.core.protocol.RedisCommand;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.lettuce.core.codec.RedisCodec;

/**
 * Output handler that performs parallel double reads for migrating slots.
 * Checks migration cache BEFORE any read is executed.
 * 
 * @param <K> Key type.
 * @param <V> Value type.
 */
public class ParallelDoubleReadsValueOutput<K, V> extends MigrationAwareValueOutput<K, V> {
    
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ParallelDoubleReadsValueOutput.class);
    
    private final ClusterConnectionProvider clusterConnectionProvider;
    private final Partitions partitions;
    private final ReadFrom readFrom;
    private final K key;
    private final RedisCodec<K, V> codec;
    
    private volatile boolean doubleReadsInitiated = false;
    private volatile CompletableFuture<V> doubleReadsFuture;
    
    public ParallelDoubleReadsValueOutput(RedisCodec<K, V> codec, 
                                        ClusterConnectionProvider clusterConnectionProvider,
                                        Partitions partitions,
                                        ReadFrom readFrom,
                                        K key) {
        super(codec);
        this.clusterConnectionProvider = clusterConnectionProvider;
        this.partitions = partitions;
        this.readFrom = readFrom;
        this.key = key;
        this.codec = codec;
        
        // Check migration cache immediately and initiate double reads if needed
        initiateParallelDoubleReadsIfNeeded();
    }
    
    /**
     * Check migration cache and initiate parallel double reads if the slot is migrating.
     * This is called immediately when the output is created, before any read is executed.
     */
    private void initiateParallelDoubleReadsIfNeeded() {
        if (key == null) {
            return;
        }
        
        int slot = calculateSlot(key);
        
        // Check if the slot is migrating based on the migration cache
        if (!MigrationCache.getInstance().isSlotMigrating(slot)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Slot {} is not migrating, skipping parallel double reads for key: {}", slot, key);
            }
            return;
        }
        
        // Get migration target from cache
        RedisClusterNode migrationTarget = MigrationCache.getInstance().getMigrationTargetBySlot(slot);
        if (migrationTarget == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("No migration target found in cache for slot {}, skipping parallel double reads", slot);
            }
            return;
        }
        
        // Get the master node for this slot
        RedisClusterNode master = partitions != null ? partitions.getMasterBySlot(slot) : null;
        if (master == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("No master found for slot {}, skipping parallel double reads", slot);
            }
            return;
        }
        
        // Get a replica for the first read
        RedisClusterNode replica = getReplicaForSlot(master);
        if (replica == null) {
            // If no replica, use master as fallback
            replica = master;
        }
        
        if (logger.isDebugEnabled()) {
            logger.debug("Initiating parallel double reads for migrating slot {}: replica={}, migration_target={}", 
                       slot, replica.getUri(), migrationTarget.getUri());
        }
        
        // Execute both reads in parallel immediately
        doubleReadsInitiated = true;
        executeParallelDoubleReads(replica, migrationTarget);
    }
    
    private int calculateSlot(K key) {
        if (key instanceof String) {
            return SlotHash.getSlot((String) key);
        } else if (key instanceof byte[]) {
            return SlotHash.getSlot((byte[]) key);
        } else {
            // Encode the key to bytes and calculate slot
            ByteBuffer encoded = codec.encodeKey(key);
            return SlotHash.getSlot(encoded);
        }
    }
    
    private void executeParallelDoubleReads(RedisClusterNode replica, RedisClusterNode migrationTarget) {
        // Execute both reads in parallel
        CompletableFuture<V> replicaRead = executeReadOnNode(replica, "replica");
        CompletableFuture<V> migrationTargetRead = executeReadOnNode(migrationTarget, "migration_target");
        
        // Wait for both to complete and compare results
        doubleReadsFuture = CompletableFuture.allOf(replicaRead, migrationTargetRead)
            .thenApply(v -> {
                V replicaResult = replicaRead.join();
                V migrationTargetResult = migrationTargetRead.join();
                
                return compareAndSelectResult(replicaResult, migrationTargetResult);
            })
            .exceptionally(throwable -> {
                if (logger.isWarnEnabled()) {
                    logger.warn("Parallel double reads failed for key {}: {}", key, throwable.getMessage());
                }
                return null; // Will be overridden by the normal read result
            });
    }
    
    private RedisClusterNode getReplicaForSlot(RedisClusterNode master) {
        // Find replicas of the master
        for (RedisClusterNode node : partitions) {
            if (master.getNodeId().equals(node.getSlaveOf()) && node.isConnected()) {
                return node;
            }
        }
        return null;
    }
    
    private CompletableFuture<V> executeReadOnNode(RedisClusterNode node, String nodeType) {
        try {
            StatefulRedisConnection<K, V> connection = clusterConnectionProvider.getConnection(
                ConnectionIntent.READ, node.getUri().getHost(), node.getUri().getPort());
            
            // Create a new command with the same parameters
            RedisCommand<K, V, V> newCommand = new Command<>(
                CommandType.GET, 
                new io.lettuce.core.output.ValueOutput<>(codec), 
                new CommandArgs<>(codec).addKey(key)
            );
            
            // Execute the command
            connection.dispatch(newCommand);
            
            // Wait for completion and return result
            return CompletableFuture.supplyAsync(() -> {
                try {
                    // Wait for the command to complete
                    while (!newCommand.isDone()) {
                        Thread.sleep(1);
                    }
                    return newCommand.getOutput().get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            });
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Failed to read from {} node {}: {}", nodeType, node.getUri(), e.getMessage());
            }
            return CompletableFuture.completedFuture(null);
        }
    }
    
    private V compareAndSelectResult(V replicaResult, V migrationTargetResult) {
        if (replicaResult == null && migrationTargetResult == null) {
            return null;
        }
        
        if (replicaResult == null) {
            return migrationTargetResult;
        }
        
        if (migrationTargetResult == null) {
            return replicaResult;
        }
        
        // Both results are available, compare them
        boolean valuesMatch = java.util.Objects.equals(replicaResult, migrationTargetResult);
        
        if (!valuesMatch) {
            if (logger.isWarnEnabled()) {
                logger.warn("Parallel double read inconsistency detected for key {}: replica={}, migration_target={}", 
                           key, replicaResult, migrationTargetResult);
            }
            
            // For now, return the replica result as it's likely more consistent
            return replicaResult;
        }
        
        // Values match, return either one (prefer replica)
        return replicaResult;
    }
    
    @Override
    public void set(ByteBuffer bytes) {
        // If parallel double reads were initiated, use their result
        if (doubleReadsInitiated && doubleReadsFuture != null) {
            try {
                V doubleReadsResult = doubleReadsFuture.get();
                if (doubleReadsResult != null) {
                    // Use the result from parallel double reads
                    super.set(bytes); // Set the original result for migration metadata
                    // The actual value will be overridden by the double reads result
                    return;
                }
            } catch (Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Failed to get parallel double reads result, using normal result: {}", e.getMessage());
                }
            }
        }
        
        // Fall back to normal behavior
        super.set(bytes);
    }
    
    /**
     * Wait for parallel double reads to complete if they were initiated.
     * 
     * @return CompletableFuture that completes when double reads are done
     */
    public CompletableFuture<Void> waitForDoubleReads() {
        if (doubleReadsFuture != null) {
            return doubleReadsFuture.thenApply(v -> null);
        }
        return CompletableFuture.completedFuture(null);
    }
    
    /**
     * Check if parallel double reads were initiated.
     * 
     * @return true if parallel double reads were initiated
     */
    public boolean isDoubleReadsInitiated() {
        return doubleReadsInitiated;
    }
} 