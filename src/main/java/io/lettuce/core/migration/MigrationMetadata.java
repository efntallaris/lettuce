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

/**
 * Migration metadata structure extracted from Redis responses.
 * 
 * According to the documentation, the metadata structure is 12 bytes:
 * - Offset 0-1: slot_id (uint16_t) - Hash slot number (0-16383)
 * - Offset 2-3: migration_status (uint16_t) - Migration status (0-2)
 * - Offset 4-7: source_id (uint32_t) - Source node configEpoch
 * - Offset 8-11: dest_id (uint32_t) - Destination node configEpoch
 *
 * @author Mark Paluch
 * @since 6.3
 */
public class MigrationMetadata {

    public enum MigrationStatus {
        NOT_MIGRATED(0),
        IN_PROGRESS(1),
        MIGRATED(2);

        private final int value;

        MigrationStatus(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static MigrationStatus fromValue(int value) {
            for (MigrationStatus status : values()) {
                if (status.value == value) {
                    return status;
                }
            }
            throw new IllegalArgumentException("Unknown migration status: " + value);
        }
    }

    private final int slotId;
    private final MigrationStatus migrationStatus;
    private final long sourceId;

    public MigrationMetadata(int slotId, MigrationStatus migrationStatus, long sourceId) {
        this.slotId = slotId;
        this.migrationStatus = migrationStatus;
        this.sourceId = sourceId;
    }

    /**
     * Parse migration metadata from a ByteBuffer containing the last 12 bytes of a Redis response.
     * 
     * @param buffer ByteBuffer containing the metadata (must have at least 12 bytes remaining)
     * @return MigrationMetadata object
     */
    public static MigrationMetadata parse(ByteBuffer buffer) {
        if (buffer.remaining() < 12) {
            throw new IllegalArgumentException("Buffer must have at least 12 bytes remaining");
        }

        // Redis uses little-endian byte order
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        
        int slotId = buffer.getShort() & 0xFFFF; // Convert to unsigned
        int statusValue = buffer.getShort() & 0xFFFF; // Convert to unsigned
        long sourceId = buffer.getInt() & 0xFFFFFFFFL; // Convert to unsigned

        return new MigrationMetadata(slotId, MigrationStatus.fromValue(statusValue), sourceId);
    }

    public int getSlotId() {
        return slotId;
    }

    public MigrationStatus getMigrationStatus() {
        return migrationStatus;
    }

    public long getSourceId() {
        return sourceId;
    }

    @Override
    public String toString() {
        return "MigrationMetadata{" +
                "slotId=" + slotId +
                ", migrationStatus=" + migrationStatus +
                ", sourceId=" + sourceId +
                '}';
    }
}
