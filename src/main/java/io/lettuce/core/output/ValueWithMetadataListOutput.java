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
import io.lettuce.core.models.ValueWithMetadata;

/**
 * List output class that returns a list of ValueWithMetadata objects containing both
 * the values and any metadata that was attached to the Redis responses.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 * @author Your Name
 * @since 7.0
 */
public class ValueWithMetadataListOutput<K, V> extends CommandOutput<K, V, List<ValueWithMetadata<V>>> {

    private final List<MetadataAwareValueOutput<K, V>> metadataAwareOutputs = new ArrayList<>();

    public ValueWithMetadataListOutput(RedisCodec<K, V> codec) {
        super(codec, new ArrayList<>());
    }

    @Override
    public void set(ByteBuffer bytes) {
        // Create a new metadata-aware output for this value
        MetadataAwareValueOutput<K, V> output = new MetadataAwareValueOutput<>(codec);
        output.set(bytes);
        metadataAwareOutputs.add(output);
        
        // Create ValueWithMetadata and add to the output list
        V value = output.get();
        ValueWithMetadata<V> valueWithMetadata = new ValueWithMetadata<>(
            value,
            output.getAttributes(),
            output.getMigrationMetadata()
        );
        this.output.add(valueWithMetadata);
    }

    /**
     * Set an attribute from RESP3 attribute response for the last value.
     * This method is called by the protocol decoder when processing RESP3 attributes.
     *
     * @param key the attribute key
     * @param value the attribute value
     */
    public void setAttribute(String key, Object value) {
        if (!metadataAwareOutputs.isEmpty()) {
            MetadataAwareValueOutput<K, V> lastOutput = metadataAwareOutputs.get(metadataAwareOutputs.size() - 1);
            lastOutput.setAttribute(key, value);
            
            // Update the last ValueWithMetadata in the output list
            if (!this.output.isEmpty()) {
                V val = this.output.get(this.output.size() - 1).getValue();
                ValueWithMetadata<V> updatedValueWithMetadata = new ValueWithMetadata<>(
                    val,
                    lastOutput.getAttributes(),
                    lastOutput.getMigrationMetadata()
                );
                this.output.set(this.output.size() - 1, updatedValueWithMetadata);
            }
        }
    }
} 