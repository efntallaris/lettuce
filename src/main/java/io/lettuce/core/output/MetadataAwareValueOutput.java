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
import java.util.HashMap;
import java.util.Map;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.models.MigrationMetadata;

/**
 * Value output that can handle RESP3 metadata attributes.
 * This output class extends ValueOutput to capture metadata that may be
 * attached to Redis responses, particularly migration metadata.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 * @author Your Name
 * @since 7.0
 */
public class MetadataAwareValueOutput<K, V> extends ValueOutput<K, V> {

    private final Map<String, Object> attributes = new HashMap<>();
    private MigrationMetadata migrationMetadata;

    public MetadataAwareValueOutput(RedisCodec<K, V> codec) {
        super(codec);
    }

    @Override
    public void set(ByteBuffer bytes) {
        super.set(bytes);
    }

    /**
     * Set an attribute from RESP3 attribute response.
     * This method is called by the protocol decoder when processing RESP3 attributes.
     *
     * @param key the attribute key
     * @param value the attribute value
     */
    public void setAttribute(String key, Object value) {
        attributes.put(key, value);
        
        // Check if this is migration metadata
        if ("slot_id".equals(key) || "migration_status".equals(key) || 
            "source_id".equals(key) || "dest_id".equals(key)) {
            updateMigrationMetadata();
        }
    }

    /**
     * Get all attributes that were attached to this response.
     *
     * @return a copy of the attributes map
     */
    public Map<String, Object> getAttributes() {
        return new HashMap<>(attributes);
    }

    /**
     * Get a specific attribute by key.
     *
     * @param key the attribute key
     * @return the attribute value, or null if not found
     */
    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    /**
     * Get the migration metadata if present.
     *
     * @return the migration metadata, or null if not present
     */
    public MigrationMetadata getMigrationMetadata() {
        return migrationMetadata;
    }

    /**
     * Check if this response has any attributes.
     *
     * @return true if attributes are present
     */
    public boolean hasAttributes() {
        return !attributes.isEmpty();
    }

    /**
     * Check if this response has migration metadata.
     *
     * @return true if migration metadata is present
     */
    public boolean hasMigrationMetadata() {
        return migrationMetadata != null && migrationMetadata.hasMigrationData();
    }

    /**
     * Update the migration metadata based on current attributes.
     */
    private void updateMigrationMetadata() {
        Integer slotId = getIntegerAttribute("slot_id");
        Integer migrationStatus = getIntegerAttribute("migration_status");
        Integer sourceId = getIntegerAttribute("source_id");
        Integer destId = getIntegerAttribute("dest_id");

        if (slotId != null || migrationStatus != null || sourceId != null || destId != null) {
            migrationMetadata = new MigrationMetadata(slotId, migrationStatus, sourceId, destId, attributes);
        }
    }

    /**
     * Helper method to get an integer attribute.
     *
     * @param key the attribute key
     * @return the integer value, or null if not found or not an integer
     */
    private Integer getIntegerAttribute(String key) {
        Object value = attributes.get(key);
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Long) {
            return ((Long) value).intValue();
        } else if (value instanceof String) {
            try {
                return Integer.valueOf((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "MetadataAwareValueOutput{" +
                "output=" + get() +
                ", attributes=" + attributes +
                ", migrationMetadata=" + migrationMetadata +
                '}';
    }
} 