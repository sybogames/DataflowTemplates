/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.IOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;

public abstract class JSONTransformer<T>
    extends PTransform<PCollection<FailsafeElement<T, String>>, PCollectionTuple> {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  public abstract TupleTag<FailsafeElement<T, String>> successTag();

  public abstract TupleTag<FailsafeElement<T, String>> failureTag();

  public static <T> Builder<T> newBuilder() {
    return new AutoValueJSONTransformer.Builder<>();
  }

  public abstract static class Builder<T> {
    public abstract Builder<T> setSuccessTag(TupleTag<FailsafeElement<T, String>> successTag);

    public abstract Builder<T> setFailureTag(TupleTag<FailsafeElement<T, String>> failureTag);

    public abstract JSONTransformer<T> build();
  }

  @Override
  public PCollectionTuple expand(PCollection<FailsafeElement<T, String>> elements) {
    return elements.apply(
        "ProcessEvents",
        ParDo.of(
                new DoFn<FailsafeElement<T, String>, FailsafeElement<T, String>>() {

                  private Set<String> keysToSkip;

                  @Setup
                  public void setup() {
                    // Using a HashSet for O(1) lookups
                    keysToSkip = new HashSet<>();
                    keysToSkip.add("event_id");
                    keysToSkip.add("event_timestamp");
                    keysToSkip.add("event_name");
                  }

                  @ProcessElement
                  public void processElement(
                      @Element FailsafeElement<T, String> event, MultiOutputReceiver out) {
                    String payloadStr = event.getPayload();
                    try {
                      String transformedJson =
                          JSONTransformer.transformJson(payloadStr, keysToSkip);
                      if (!Strings.isNullOrEmpty(transformedJson)) {
                        out.get(successTag())
                            .output(
                                FailsafeElement.of(event.getOriginalPayload(), transformedJson));
                      }
                    } catch (Throwable e) {
                      out.get(failureTag())
                          .output(
                              FailsafeElement.of(event)
                                  .setErrorMessage(e.getMessage())
                                  .setStacktrace(Throwables.getStackTraceAsString(e)));
                    }
                  }
                })
            .withOutputTags(successTag(), TupleTagList.of(failureTag())));
  }

  public static JsonNode transformJson(JsonNode originalNode, Set<String> keysToSkip) {
    ObjectNode transformedJson = objectMapper.createObjectNode();

    originalNode
        .fieldNames()
        .forEachRemaining(
            key -> {
              JsonNode value = originalNode.get(key);

              // If the key name suggests it's a timestamp, convert it if numeric
              if (key.contains("timestamp")) {
                long timestampValue = value.asLong();
                int length = String.valueOf(timestampValue).length();
                Instant instant = null;
                switch (length) {
                  case 13:
                    instant = Instant.ofEpochMilli(timestampValue);
                    break;
                  case 10:
                    instant = Instant.ofEpochSecond(timestampValue);
                    break;
                  case 16:
                    instant =
                        Instant.ofEpochSecond(
                            timestampValue / 1_000_000, (timestampValue % 1_000_000) * 1_000);
                    break;
                }
                if (instant != null) {
                  // Store timestamp as an ISO-8601 string
                  transformedJson.put(key, instant.toString());
                  return;
                }
              }

              // If this key should be left as-is, just copy it directly
              if (keysToSkip.contains(key)) {
                transformedJson.set(key, value);
                return;
              }

              // At this point, for non-skipped fields, we want them as strings.
              // If it's textual but not "null", keep as text.
              if (value.isTextual()) {
                String textValue = value.asText();
                if ("null".equals(textValue)) {
                  transformedJson.set(key, objectMapper.nullNode());
                } else {
                  // Keep textual value as is
                  transformedJson.set(key, value);
                }
              } else {
                // For non-textual values (including nested objects/arrays),
                // convert the entire node to its JSON string representation.
                // This ensures complex nested data fits into a single column as a string.
                if (value.isNull()) {
                  transformedJson.set(key, objectMapper.nullNode());
                } else {
                  transformedJson.put(key, value.toString());
                }
              }
            });

    // Add a processing timestamp
    String processingTimestampAsString =
        Instant.ofEpochMilli(System.currentTimeMillis()).toString();
    transformedJson.put("processing_timestamp", processingTimestampAsString);

    return transformedJson;
  }

  public static String transformJson(String jsonString, Set<String> keysToSkip) throws IOException {
    JsonNode originalNode = objectMapper.readTree(jsonString);
    JsonNode transformedNode = transformJson(originalNode, keysToSkip);
    return objectMapper.writeValueAsString(transformedNode);
  }
}
