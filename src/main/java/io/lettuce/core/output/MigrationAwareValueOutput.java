/*
 * Copyright 2011-Present, Redis Ltd. and Contributors
 * All rights reserved.
 *
 * Licensed under the MIT License.
 *
 * This file contains contributions from third-party contributors
 * licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lettuce.core.output;

import java.nio.ByteBuffer;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.migration.MigrationAwareResponse;
import io.lettuce.core.migration.MigrationMetadata;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.lettuce.core.cluster.models.partitions.Partitions;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;

/**
 * Value output that extracts migration metadata from the last 50 bytes of Redis responses.
 * 
 * This output handler follows the pattern where Redis includes migration metadata
 * as the last 50 bytes of bulk string responses. The metadata structure is:
 * - 2 bytes: slot_id (uint16_t)
 * - 2 bytes: migration_status (uint16_t) 
 * - 46 bytes: host (char array, MAX_HOST_LEN)
 * - 2 bytes: port (uint16_t)
 *
 * This output handler extracts migration metadata from Redis responses
 * @param <K> Key type.
 * @param <V> Value type.
 * @author Mark Paluch
 * @since 6.3
 */
public class MigrationAwareValueOutput<K, V> extends CommandOutput<K, V, MigrationAwareResponse<V>> {

    private static final int METADATA_SIZE = 52; // Updated to match new metadata structure: 2+2+46+2 bytes
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(MigrationAwareValueOutput.class);
    private V originalValue;
    private Partitions partitions;
    private int slot;


    public MigrationAwareValueOutput(RedisCodec<K, V> codec) {
        super(codec, null);
    }

    @Override
    public void set(ByteBuffer bytes) {
        if (logger.isDebugEnabled()) {
            logger.debug("IN MIGRATION AWARE VALUE OUTPUT: Setting value with bytes: {}", bytes);
        }
        if (bytes == null) {
            output = new MigrationAwareResponse<>(null, null);
            return;
        }

        // Check if the buffer is large enough to contain metadata
        if (bytes.remaining() >= METADATA_SIZE) {
            if (logger.isDebugEnabled()) {
                logger.debug("IN MIGRATION AWARE VALUE OUTPUT: Buffer is large enough to contain metadata");
            }
            // Calculate the boundary between data and metadata
            int dataLength = bytes.remaining() - METADATA_SIZE;
            if (logger.isDebugEnabled()) {
                logger.debug("IN MIGRATION AWARE VALUE OUTPUT: Data length: {}", dataLength);
            }

            // Extract the original data (first N bytes)
            ByteBuffer dataBuffer = bytes.duplicate();
            dataBuffer.limit(dataBuffer.position() + dataLength);
            originalValue = codec.decodeValue(dataBuffer);
            if (logger.isDebugEnabled()) {
                logger.debug("IN MIGRATION AWARE VALUE OUTPUT: Original value: {}", originalValue);
            }

            // Extract the metadata (last 12 bytes)
            ByteBuffer metadataBuffer = bytes.duplicate();
            metadataBuffer.position(metadataBuffer.position() + dataLength);
            metadataBuffer.limit(metadataBuffer.position() + METADATA_SIZE);
            if (logger.isDebugEnabled()) {
                logger.debug("IN MIGRATION AWARE VALUE OUTPUT: Metadata buffer (hex): {}", byteBufferToHex(metadataBuffer));
            }
            try {
                MigrationMetadata metadata = MigrationMetadata.parse(metadataBuffer);
                output = new MigrationAwareResponse<>(originalValue, metadata);
                
                // Update migration cache with the received metadata
                if (partitions != null && metadata != null && metadata.getMigrationStatus() == 1) {
                    // Use the new method that creates RedisClusterNode from MigrationMetadata
                    partitions.setMigrationTarget(slot, metadata);
                    
                    if (logger.isDebugEnabled()) {
                        logger.debug("Updated migration cache for slot {} with metadata: {}", slot, metadata);
                    }
                }
                
                // Perform parallel double reads if migration is detected and we have the necessary components
                // if (!doubleReadsPerformed && partitions != null && partitions.isSlotMigrating(slot)) {
                    // performParallelDoubleReads();
                // }

                

            } catch (Exception e) {
                // If metadata parsing fails, treat as normal response without metadata
                output = new MigrationAwareResponse<>(originalValue, null);
            }
        } else {
            // Buffer too small for metadata, treat as normal response
            originalValue = codec.decodeValue(bytes);
            output = new MigrationAwareResponse<>(originalValue, null);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append(" [MigrationAwareOutput=").append(output);
        sb.append(", metadata=").append(output.getMetadata());
        sb.append(", error='").append(error).append('\'');
        sb.append(']');
        return sb.toString();
    }

    private static String byteBufferToHex(ByteBuffer buffer) {
        ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
        readOnlyBuffer.rewind();
        StringBuilder sb = new StringBuilder();
        while (readOnlyBuffer.hasRemaining()) {
            sb.append(String.format("%02x", readOnlyBuffer.get()));
        }
        return sb.toString();
    }
} 