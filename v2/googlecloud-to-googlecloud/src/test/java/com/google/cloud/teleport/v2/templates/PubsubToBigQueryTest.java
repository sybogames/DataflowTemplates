/*
 * Copyright (C) 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.templates.PubSubToBigQuery.PubsubMessageToTableRow;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import java.io.Serializable;
import java.util.Map;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

/** Test cases for the {@link PubSubToBigQuery} class. */
public class PubsubToBigQueryTest implements Serializable {

    private static final long serialVersionUID = 1L;

    @Rule public final transient TestPipeline pipeline = TestPipeline.create();

    private static final String RESOURCES_DIR = "JavascriptTextTransformerTest/";

    private static final String TRANSFORM_FILE_PATH =
            Resources.getResource(RESOURCES_DIR + "transform.js").getPath();

    /** Tests the {@link PubSubToBigQuery} pipeline end-to-end. */
    @Test
    public void testPubsubToBigQueryE2E() throws Exception {
        // Test input
        final String payload = "{\"ticker\": \"GOOGL\", \"price\": 1006.94, \"event_timestamp\": 1698179812609,\"revenue\":0.001400002330012, \"server_timestamp\": 1698179812994015, \"events\": \"{\\\"ad_unit\\\":\\\"banner\\\",\\\"error_code\\\":601,\\\"error_type\\\":\\\"Init() had failed\\\",\\\"revenue\\\":0.001400002330012}\"}";
        final PubsubMessage message =
                new PubsubMessage(payload.getBytes(), ImmutableMap.of("id", "123", "type", "custom_event"));

        final Instant timestamp =
                new DateTime(2022, 2, 22, 22, 22, 22, 222, DateTimeZone.UTC).toInstant();

        final FailsafeElementCoder<PubsubMessage, String> coder =
                FailsafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), StringUtf8Coder.of());

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

        // Parameters
        PubSubToBigQuery.Options options =
                PipelineOptionsFactory.create().as(PubSubToBigQuery.Options.class);

        // Build pipeline
        PCollectionTuple transformOut =
                pipeline
                        .apply(
                                "CreateInput",
                                Create.timestamped(TimestampedValue.of(message, timestamp))
                                        .withCoder(PubsubMessageWithAttributesCoder.of()))
                        .apply("ConvertMessageToTableRow", new PubsubMessageToTableRow(options));

        // Assert
        PAssert.that(transformOut.get(PubSubToBigQuery.UDF_DEADLETTER_OUT)).empty();
        PAssert.that(transformOut.get(PubSubToBigQuery.TRANSFORM_DEADLETTER_OUT)).empty();
        PAssert.that(transformOut.get(PubSubToBigQuery.TRANSFORM_OUT))
                .satisfies(
                        collection -> {
                            TableRow result = collection.iterator().next();
                            assertThat(result.get("ticker"), is(equalTo("GOOGL")));
                            assertThat(result.get("price"), is(equalTo("1006.94")));
                            assertThat(result.get("revenue"), is(equalTo("0.001400002330012")));
                            assertThat(result.get("event_timestamp"), is(equalTo("2023-10-24T20:36:52.609Z")));
                            assertThat(result.get("server_timestamp"), is(equalTo("2023-10-24T20:36:52.994015Z")));
                            assertThat(result.get("events"), is(equalTo("{\"ad_unit\":\"banner\",\"error_code\":601,\"error_type\":\"Init() had failed\",\"revenue\":0.001400002330012}")));
                            return null;
                        });

        // Execute pipeline
        pipeline.run();
    }

    /** Tests the QA filtering logic in the PubSubToBigQuery pipeline. */
    @Test
    public void testQAFilteringWithMatchingUser() throws Exception {
        // Test input message mimicking real-life data
        final String payload = "{\"user_id\": \"u123\", \"activity\": \"login\", \"timestamp\": \"2022-10-24T20:36:52Z\"}";
        final PubsubMessage message =
                new PubsubMessage(payload.getBytes(), ImmutableMap.of("id", "123", "type", "user_activity"));

        final Instant timestamp = Instant.parse("2022-10-24T20:36:52Z");

        // Setting up coders
        final FailsafeElementCoder<PubsubMessage, String> coder =
                FailsafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), StringUtf8Coder.of());

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

        // Parameters
        PubSubToBigQuery.Options options =
                PipelineOptionsFactory.create().as(PubSubToBigQuery.Options.class);
        options.setOutputTableSpec("test_project:test_dataset.test_table");

        // Simulate QA user data that matches the 'user_id' in the incoming message
        PCollection<TableRow> qaUsers = pipeline.apply("CreateQAUserInput", Create.<TableRow>of(
                new TableRow().set("user_id", "u123").set("category", "premium")
        ));

        // Create a PCollectionView of the filtered rows as a Map
        PCollectionView<Map<String, TableRow>> qaUsersView = qaUsers
                .apply("ExtractUserIds", ParDo.of(new DoFn<TableRow, Map<String, TableRow>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        TableRow row = c.element();
                        c.output(ImmutableMap.of((String) row.get("user_id"), row));
                    }
                }))
                .apply("CreateQAUsersView", View.asSingleton());

        // Build and execute pipeline
        PCollectionTuple transformOut =
                pipeline
                        .apply(
                                "CreateInput",
                                Create.timestamped(TimestampedValue.of(message, timestamp))
                                        .withCoder(PubsubMessageWithAttributesCoder.of()))
                        .apply("ConvertMessageToTableRow", new PubsubMessageToTableRow(options));

        PCollection<TableRow> filteredRows = transformOut.get(PubSubToBigQuery.TRANSFORM_OUT)
                .apply("FilterAndMergeQAUsers", ParDo.of(new PubSubToBigQuery.FilterAndMergeQAUsersFn(qaUsersView)).withSideInputs(qaUsersView));

        // Assert that the merged rows include QA user data
        PAssert.that(filteredRows)
                .satisfies(
                        collection -> {
                            TableRow result = collection.iterator().next();
                            assertThat(result.get("user_id"), is(equalTo("u123")));
                            assertThat(result.get("activity"), is(equalTo("login")));
                            return null;
                        });

        // Execute pipeline
        pipeline.run();
    }
}