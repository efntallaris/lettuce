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

/**
 * Response wrapper that contains both the original Redis value and migration metadata.
 *
 * @param <V> Value type
 * @author Mark Paluch
 * @since 6.3
 */
public class MigrationAwareResponse<V> {

    private final V value;
    private final MigrationMetadata metadata;

    public MigrationAwareResponse(V value, MigrationMetadata metadata) {
        this.value = value;
        this.metadata = metadata;
    }

    /**
     * Get the original Redis value.
     * 
     * @return the value, or null if the key doesn't exist
     */
    public V getValue() {
        return value;
    }

    /**
     * Get the migration metadata.
     * 
     * @return the migration metadata, or null if no metadata was present
     */
    public MigrationMetadata getMetadata() {
        return metadata;
    }

    /**
     * Check if migration metadata is present.
     * 
     * @return true if metadata is present, false otherwise
     */
    public boolean hasMetadata() {
        return metadata != null;
    }

    @Override
    public String toString() {
        return "MigrationAwareResponse{" +
                "value=" + value +
                ", metadata=" + metadata +
                '}';
    }
}