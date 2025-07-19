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
import java.util.ArrayList;
import java.util.List;

import io.lettuce.core.codec.RedisCodec;

/**
 * List output that can handle RESP3 metadata attributes for each value in the list.
 * This output class extends ValueListOutput to capture metadata that may be
 * attached to Redis responses, particularly migration metadata.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 * @author Your Name
 * @since 7.0
 */
public class MetadataAwareValueListOutput<K, V> extends ValueListOutput<K, V> {

    private final List<MetadataAwareValueOutput<K, V>> metadataAwareOutputs = new ArrayList<>();

    public MetadataAwareValueListOutput(RedisCodec<K, V> codec) {
        super(codec);
    }

    @Override
    public void set(ByteBuffer bytes) {
        // Create a new metadata-aware output for this value
        MetadataAwareValueOutput<K, V> output = new MetadataAwareValueOutput<>(codec);
        output.set(bytes);
        metadataAwareOutputs.add(output);
        
        // Also set the value in the parent list
        super.set(bytes);
    }

    /**
     * Get the list of metadata-aware outputs.
     *
     * @return the list of metadata-aware outputs
     */
    public List<MetadataAwareValueOutput<K, V>> getMetadataAwareOutputs() {
        return new ArrayList<>(metadataAwareOutputs);
    }

    /**
     * Get a specific metadata-aware output by index.
     *
     * @param index the index
     * @return the metadata-aware output, or null if index is out of bounds
     */
    public MetadataAwareValueOutput<K, V> getMetadataAwareOutput(int index) {
        if (index >= 0 && index < metadataAwareOutputs.size()) {
            return metadataAwareOutputs.get(index);
        }
        return null;
    }

    /**
     * Check if any value in the list has attributes.
     *
     * @return true if any value has attributes
     */
    public boolean hasAnyAttributes() {
        return metadataAwareOutputs.stream().anyMatch(MetadataAwareValueOutput::hasAttributes);
    }

    /**
     * Check if any value in the list has migration metadata.
     *
     * @return true if any value has migration metadata
     */
    public boolean hasAnyMigrationMetadata() {
        return metadataAwareOutputs.stream().anyMatch(MetadataAwareValueOutput::hasMigrationMetadata);
    }

    /**
     * Get all migration metadata from all values in the list.
     *
     * @return list of migration metadata (null values for items without metadata)
     */
    public List<io.lettuce.core.models.MigrationMetadata> getAllMigrationMetadata() {
        List<io.lettuce.core.models.MigrationMetadata> result = new ArrayList<>();
        for (MetadataAwareValueOutput<K, V> output : metadataAwareOutputs) {
            result.add(output.getMigrationMetadata());
        }
        return result;
    }

    @Override
    public String toString() {
        return "MetadataAwareValueListOutput{" +
                "output=" + get() +
                ", metadataAwareOutputs=" + metadataAwareOutputs +
                '}';
    }
} 