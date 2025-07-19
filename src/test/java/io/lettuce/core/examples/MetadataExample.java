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
package io.lettuce.core.examples;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.output.MetadataAwareValueOutput;
import io.lettuce.core.models.MigrationMetadata;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Example demonstrating how to use the metadata functionality in Lettuce.
 * This example shows how to handle RESP3 attributes that may be attached to Redis responses.
 *
 * @author Your Name
 * @since 7.0
 */
public class MetadataExample {

    public static void main(String[] args) {

        // Create Redis client
        RedisClient client = RedisClient.create(RedisURI.create("localhost", 6379));
        StatefulRedisConnection<String, String> connection = client.connect();
        RedisAsyncCommands<String, String> commands = connection.async();

        try {
            // Example 1: Get a single value with metadata
            CompletableFuture<Void> singleGetExample = commands.getWithMetadata("mykey")
                    .thenAccept(result -> {
                        System.out.println("Value: " + result.get());
                        
                        if (result.hasAttributes()) {
                            System.out.println("Attributes: " + result.getAttributes());
                        }
                        
                        if (result.hasMigrationMetadata()) {
                            MigrationMetadata metadata = result.getMigrationMetadata();
                            System.out.println("Migration Metadata:");
                            System.out.println("  Slot ID: " + metadata.getSlotId());
                            System.out.println("  Migration Status: " + metadata.getMigrationStatus());
                            System.out.println("  Source ID: " + metadata.getSourceId());
                            System.out.println("  Destination ID: " + metadata.getDestId());
                        }
                    });

            // Example 2: Get multiple values with metadata
            CompletableFuture<Void> multiGetExample = commands.mgetWithMetadata("key1", "key2", "key3")
                    .thenAccept(results -> {
                        System.out.println("\nMultiple values with metadata:");
                        
                        for (int i = 0; i < results.size(); i++) {
                            MetadataAwareValueOutput<String, String> result = results.get(i);
                            System.out.println("Key " + i + ": " + result.get());
                            
                            if (result.hasMigrationMetadata()) {
                                MigrationMetadata metadata = result.getMigrationMetadata();
                                System.out.println("  Has migration metadata: slot=" + metadata.getSlotId());
                            }
                        }
                    });

            // Example 3: Check for specific attributes
            CompletableFuture<Void> attributeCheckExample = commands.getWithMetadata("mykey")
                    .thenAccept(result -> {
                        // Check for specific attributes
                        Object slotId = result.getAttribute("slot_id");
                        Object migrationStatus = result.getAttribute("migration_status");
                        
                        if (slotId != null) {
                            System.out.println("Slot ID attribute: " + slotId);
                        }
                        
                        if (migrationStatus != null) {
                            System.out.println("Migration status attribute: " + migrationStatus);
                        }
                    });

            // Wait for all examples to complete
            CompletableFuture.allOf(singleGetExample, multiGetExample, attributeCheckExample).join();

        } finally {
            connection.close();
            client.shutdown();
        }
    }
} 