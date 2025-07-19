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
import io.lettuce.core.models.ValueWithMetadata;

/**
 * Output class that returns ValueWithMetadata objects containing both the value
 * and any metadata that was attached to the Redis response.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 * @author Your Name
 * @since 7.0
 */
public class ValueWithMetadataOutput<K, V> extends CommandOutput<K, V, ValueWithMetadata<V>> {

    private final MetadataAwareValueOutput<K, V> metadataAwareOutput;

    public ValueWithMetadataOutput(RedisCodec<K, V> codec) {
        super(codec, null);
        this.metadataAwareOutput = new MetadataAwareValueOutput<>(codec);
    }

    @Override
    public void set(ByteBuffer bytes) {
        metadataAwareOutput.set(bytes);
        V value = metadataAwareOutput.get();
        this.output = new ValueWithMetadata<>(
            value,
            metadataAwareOutput.getAttributes(),
            metadataAwareOutput.getMigrationMetadata()
        );
    }

    /**
     * Set an attribute from RESP3 attribute response.
     * This method is called by the protocol decoder when processing RESP3 attributes.
     *
     * @param key the attribute key
     * @param value the attribute value
     */
    public void setAttribute(String key, Object value) {
        metadataAwareOutput.setAttribute(key, value);
        // Update the output with the new metadata
        if (this.output != null) {
            V val = this.output.getValue();
            this.output = new ValueWithMetadata<>(
                val,
                metadataAwareOutput.getAttributes(),
                metadataAwareOutput.getMigrationMetadata()
            );
        }
    }
} 