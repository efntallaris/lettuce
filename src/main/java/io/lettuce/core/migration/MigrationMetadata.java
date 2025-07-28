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
package io.lettuce.core.migration;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * Migration metadata structure that matches the C struct in migration.h
 * 
 * C struct:
 * typedef struct migrationMetadata {
 *     uint16_t slot_id;           // 2 bytes
 *     uint16_t migration_status;  // 2 bytes  
 *     char host[MAX_HOST_LEN];    // 46 bytes (MAX_HOST_LEN = 46)
 *     uint16_t port;              // 2 bytes
 * } migrationMetadata;
 * 
 * Total size: 50 bytes
 */
public class MigrationMetadata {
    
    private static final int METADATA_SIZE = 50; // 2+2+46+2 bytes

    private static final int MAX_HOST_LEN = 46;
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(MigrationMetadata.class);
    
    private final int slotId;
    private final int migrationStatus;
    private final String host;
    private final int port;
    
    /**
     * Parse migration metadata from a ByteBuffer containing the metadata.
     * 
     * @param buffer ByteBuffer containing the metadata (must have at least METADATA_SIZE bytes remaining)
     * @return MigrationMetadata object, or null if parsing fails
     */
    public static MigrationMetadata parse(ByteBuffer buffer) {
        if (buffer == null || buffer.remaining() < METADATA_SIZE) {
            if (logger.isDebugEnabled()) {
                logger.debug("Buffer is null or too small. remaining={}, required={}", 
                    buffer != null ? buffer.remaining() : "null", METADATA_SIZE);
            }
            return null;
        }
        
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Starting to parse migration metadata. Buffer remaining: {}", buffer.remaining());
            }
            
            ByteBuffer bb = buffer.duplicate();
            bb.order(ByteOrder.LITTLE_ENDIAN);
            
            // Read slot_id (2 bytes, uint16_t)
            int slotId = bb.getShort() & 0xFFFF;
            if (logger.isDebugEnabled()) {
                logger.debug("Read slotId: {}", slotId);
            }
            
            // Read migration_status (2 bytes, uint16_t)  
            int migrationStatus = bb.getShort() & 0xFFFF;
            if (logger.isDebugEnabled()) {
                logger.debug("Read migrationStatus: {}", migrationStatus);
            }
            
            // Read host (46 bytes, char array)
            byte[] hostBytes = new byte[MAX_HOST_LEN];
            bb.get(hostBytes);
            String host = new String(hostBytes).trim();
            if (logger.isDebugEnabled()) {
                logger.debug("Read host: '{}' (length: {})", host, host.length());
            }
            
            // Read port (2 bytes, uint16_t)
            int port = bb.getShort() & 0xFFFF;
            if (logger.isDebugEnabled()) {
                logger.debug("Read port: {}", port);
            }
            
            if (logger.isDebugEnabled()) {
                logger.debug("Successfully parsed migration metadata");
            }
            return new MigrationMetadata(slotId, migrationStatus, host, port);
            
        } catch (Exception e) {
            logger.warn("Exception while parsing migration metadata: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * Parse migration metadata from a byte buffer
     * 
     * @param buffer The buffer containing the data + metadata
     * @param dataLength The length of the actual data (before metadata)
     * @return MigrationMetadata object, or null if parsing fails
     */
    public static MigrationMetadata parse(byte[] buffer, int dataLength) {
        if (buffer == null || buffer.length < dataLength + METADATA_SIZE) {
            return null;
        }
        
        try {
            ByteBuffer bb = ByteBuffer.wrap(buffer, dataLength, METADATA_SIZE);
            bb.order(ByteOrder.LITTLE_ENDIAN);
            
            // Read slot_id (2 bytes, uint16_t)
            int slotId = bb.getShort() & 0xFFFF;
            
            // Read migration_status (2 bytes, uint16_t)  
            int migrationStatus = bb.getShort() & 0xFFFF;
            
            // Read host (46 bytes, char array)
            byte[] hostBytes = new byte[MAX_HOST_LEN];
            bb.get(hostBytes);
            String host = new String(hostBytes).trim();
            
            // Read port (2 bytes, uint16_t)
            int port = bb.getShort() & 0xFFFF;
            
            return new MigrationMetadata(slotId, migrationStatus, host, port);
            
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Extract the actual data from a buffer containing data + metadata
     * 
     * @param buffer The buffer containing data + metadata
     * @param dataLength The length of the actual data
     * @return The data portion as a byte array
     */
    public static byte[] extractData(byte[] buffer, int dataLength) {
        if (buffer == null || buffer.length < dataLength) {
            return null;
        }
        
        byte[] data = new byte[dataLength];
        System.arraycopy(buffer, 0, data, 0, dataLength);
        return data;
    }
    
    public MigrationMetadata(int slotId, int migrationStatus, String host, int port) {
        this.slotId = slotId;
        this.migrationStatus = migrationStatus;
        this.host = host;
        this.port = port;
    }
    
    public int getSlotId() {
        return slotId;
    }
    
    public int getMigrationStatus() {
        return migrationStatus;
    }
    
    public String getHost() {
        return host;
    }
    
    public int getPort() {
        return port;
    }
    
    @Override
    public String toString() {
        return String.format("MigrationMetadata{slotId=%d, migrationStatus=%d, host='%s', port=%d}", 
                           slotId, migrationStatus, host, port);
    }
}
