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
package io.lettuce.core.models;

import java.util.Map;
import java.util.HashMap;
import java.util.Objects;

/**
 * Represents migration metadata that can be attached to Redis responses.
 * This metadata includes information about cluster slot migration status.
 *
 * @author Your Name
 * @since 7.0
 */
public class MigrationMetadata {

    private final Integer slotId;
    private final Integer migrationStatus;
    private final Integer sourceId;
    private final Integer destId;
    private final Map<String, Object> additionalAttributes;

    /**
     * Create a new MigrationMetadata instance.
     *
     * @param slotId the cluster slot ID
     * @param migrationStatus the migration status
     * @param sourceId the source node ID
     * @param destId the destination node ID
     */
    public MigrationMetadata(Integer slotId, Integer migrationStatus, Integer sourceId, Integer destId) {
        this(slotId, migrationStatus, sourceId, destId, new HashMap<>());
    }

    /**
     * Create a new MigrationMetadata instance with additional attributes.
     *
     * @param slotId the cluster slot ID
     * @param migrationStatus the migration status
     * @param sourceId the source node ID
     * @param destId the destination node ID
     * @param additionalAttributes additional metadata attributes
     */
    public MigrationMetadata(Integer slotId, Integer migrationStatus, Integer sourceId, Integer destId,
            Map<String, Object> additionalAttributes) {
        this.slotId = slotId;
        this.migrationStatus = migrationStatus;
        this.sourceId = sourceId;
        this.destId = destId;
        this.additionalAttributes = new HashMap<>(additionalAttributes);
    }

    /**
     * Get the cluster slot ID.
     *
     * @return the slot ID
     */
    public Integer getSlotId() {
        return slotId;
    }

    /**
     * Get the migration status.
     *
     * @return the migration status
     */
    public Integer getMigrationStatus() {
        return migrationStatus;
    }

    /**
     * Get the source node ID.
     *
     * @return the source node ID
     */
    public Integer getSourceId() {
        return sourceId;
    }

    /**
     * Get the destination node ID.
     *
     * @return the destination node ID
     */
    public Integer getDestId() {
        return destId;
    }

    /**
     * Get additional attributes.
     *
     * @return a copy of the additional attributes map
     */
    public Map<String, Object> getAdditionalAttributes() {
        return new HashMap<>(additionalAttributes);
    }

    /**
     * Get a specific additional attribute.
     *
     * @param key the attribute key
     * @return the attribute value, or null if not found
     */
    public Object getAttribute(String key) {
        return additionalAttributes.get(key);
    }

    /**
     * Check if this metadata has any migration information.
     *
     * @return true if any migration data is present
     */
    public boolean hasMigrationData() {
        return slotId != null || migrationStatus != null || sourceId != null || destId != null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MigrationMetadata that = (MigrationMetadata) o;
        return Objects.equals(slotId, that.slotId) &&
                Objects.equals(migrationStatus, that.migrationStatus) &&
                Objects.equals(sourceId, that.sourceId) &&
                Objects.equals(destId, that.destId) &&
                Objects.equals(additionalAttributes, that.additionalAttributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(slotId, migrationStatus, sourceId, destId, additionalAttributes);
    }

    @Override
    public String toString() {
        return "MigrationMetadata{" +
                "slotId=" + slotId +
                ", migrationStatus=" + migrationStatus +
                ", sourceId=" + sourceId +
                ", destId=" + destId +
                ", additionalAttributes=" + additionalAttributes +
                '}';
    }
} 