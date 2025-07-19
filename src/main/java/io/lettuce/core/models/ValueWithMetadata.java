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
import java.util.Objects;

/**
 * Wrapper class that contains both a value and metadata from a Redis response.
 * This is used to return both the actual value and any metadata that was attached
 * to the response, such as migration metadata.
 *
 * @param <V> Value type.
 * @author Your Name
 * @since 7.0
 */
public class ValueWithMetadata<V> {

    private final V value;
    private final Map<String, Object> attributes;
    private final MigrationMetadata migrationMetadata;

    /**
     * Create a new ValueWithMetadata instance.
     *
     * @param value the actual value from Redis
     * @param attributes any attributes that were attached to the response
     * @param migrationMetadata migration metadata if present
     */
    public ValueWithMetadata(V value, Map<String, Object> attributes, MigrationMetadata migrationMetadata) {
        this.value = value;
        this.attributes = attributes;
        this.migrationMetadata = migrationMetadata;
    }

    /**
     * Get the actual value from Redis.
     *
     * @return the value
     */
    public V getValue() {
        return value;
    }

    /**
     * Get all attributes that were attached to this response.
     *
     * @return a copy of the attributes map
     */
    public Map<String, Object> getAttributes() {
        return attributes;
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
        return attributes != null && !attributes.isEmpty();
    }

    /**
     * Check if this response has migration metadata.
     *
     * @return true if migration metadata is present
     */
    public boolean hasMigrationMetadata() {
        return migrationMetadata != null && migrationMetadata.hasMigrationData();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValueWithMetadata<?> that = (ValueWithMetadata<?>) o;
        return Objects.equals(value, that.value) &&
                Objects.equals(attributes, that.attributes) &&
                Objects.equals(migrationMetadata, that.migrationMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, attributes, migrationMetadata);
    }

    @Override
    public String toString() {
        return "ValueWithMetadata{" +
                "value=" + value +
                ", attributes=" + attributes +
                ", migrationMetadata=" + migrationMetadata +
                '}';
    }
} 