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

import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.models.MigrationMetadata;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link MetadataAwareValueOutput}.
 *
 * @author Your Name
 * @since 7.0
 */
class MetadataAwareValueOutputUnitTests {

    private final MetadataAwareValueOutput<String, String> output = new MetadataAwareValueOutput<>(StringCodec.UTF8);

    @Test
    void shouldSetValue() {
        String value = "test-value";
        output.set(ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)));
        
        assertThat(output.getValue()).isEqualTo(value);
    }

    @Test
    void shouldSetAttributes() {
        output.setAttribute("slot_id", 123);
        output.setAttribute("migration_status", 1);
        output.setAttribute("source_id", 456);
        output.setAttribute("dest_id", 789);
        output.setAttribute("custom_attr", "custom_value");
        
        assertThat(output.hasAttributes()).isTrue();
        assertThat(output.getAttributes()).containsKeys("slot_id", "migration_status", "source_id", "dest_id", "custom_attr");
        assertThat(output.getAttribute("slot_id")).isEqualTo(123);
        assertThat(output.getAttribute("custom_attr")).isEqualTo("custom_value");
    }

    @Test
    void shouldCreateMigrationMetadata() {
        output.setAttribute("slot_id", 123);
        output.setAttribute("migration_status", 1);
        output.setAttribute("source_id", 456);
        output.setAttribute("dest_id", 789);
        
        assertThat(output.hasMigrationMetadata()).isTrue();
        
        MigrationMetadata metadata = output.getMigrationMetadata();
        assertThat(metadata).isNotNull();
        assertThat(metadata.getSlotId()).isEqualTo(123);
        assertThat(metadata.getMigrationStatus()).isEqualTo(1);
        assertThat(metadata.getSourceId()).isEqualTo(456);
        assertThat(metadata.getDestId()).isEqualTo(789);
        assertThat(metadata.hasMigrationData()).isTrue();
    }

    @Test
    void shouldHandlePartialMigrationMetadata() {
        output.setAttribute("slot_id", 123);
        output.setAttribute("migration_status", 1);
        // Missing source_id and dest_id
        
        assertThat(output.hasMigrationMetadata()).isTrue();
        
        MigrationMetadata metadata = output.getMigrationMetadata();
        assertThat(metadata).isNotNull();
        assertThat(metadata.getSlotId()).isEqualTo(123);
        assertThat(metadata.getMigrationStatus()).isEqualTo(1);
        assertThat(metadata.getSourceId()).isNull();
        assertThat(metadata.getDestId()).isNull();
        assertThat(metadata.hasMigrationData()).isTrue();
    }

    @Test
    void shouldHandleNoMigrationMetadata() {
        output.setAttribute("custom_attr", "custom_value");
        
        assertThat(output.hasAttributes()).isTrue();
        assertThat(output.hasMigrationMetadata()).isFalse();
        assertThat(output.getMigrationMetadata()).isNull();
    }

    @Test
    void shouldHandleNoAttributes() {
        assertThat(output.hasAttributes()).isFalse();
        assertThat(output.hasMigrationMetadata()).isFalse();
        assertThat(output.getAttributes()).isEmpty();
        assertThat(output.getMigrationMetadata()).isNull();
    }

    @Test
    void shouldHandleStringAttributes() {
        output.setAttribute("slot_id", "123");
        output.setAttribute("migration_status", "1");
        
        assertThat(output.hasMigrationMetadata()).isTrue();
        
        MigrationMetadata metadata = output.getMigrationMetadata();
        assertThat(metadata.getSlotId()).isEqualTo(123);
        assertThat(metadata.getMigrationStatus()).isEqualTo(1);
    }

    @Test
    void shouldHandleLongAttributes() {
        output.setAttribute("slot_id", 123L);
        output.setAttribute("migration_status", 1L);
        
        assertThat(output.hasMigrationMetadata()).isTrue();
        
        MigrationMetadata metadata = output.getMigrationMetadata();
        assertThat(metadata.getSlotId()).isEqualTo(123);
        assertThat(metadata.getMigrationStatus()).isEqualTo(1);
    }

    @Test
    void shouldReturnCopyOfAttributes() {
        output.setAttribute("key1", "value1");
        
        Map<String, Object> attributes1 = output.getAttributes();
        Map<String, Object> attributes2 = output.getAttributes();
        
        assertThat(attributes1).isNotSameAs(attributes2);
        assertThat(attributes1).isEqualTo(attributes2);
    }

    @Test
    void shouldReturnCopyOfAdditionalAttributes() {
        output.setAttribute("slot_id", 123);
        output.setAttribute("custom_attr", "custom_value");
        
        MigrationMetadata metadata = output.getMigrationMetadata();
        Map<String, Object> additionalAttributes = metadata.getAdditionalAttributes();
        
        assertThat(additionalAttributes).containsKeys("slot_id", "custom_attr");
        assertThat(additionalAttributes.get("slot_id")).isEqualTo(123);
        assertThat(additionalAttributes.get("custom_attr")).isEqualTo("custom_value");
    }
} 