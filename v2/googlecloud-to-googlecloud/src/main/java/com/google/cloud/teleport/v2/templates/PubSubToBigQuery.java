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

import static com.google.cloud.teleport.v2.templates.TextToBigQueryStreaming.wrapBigQueryInsertError;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.options.BigQueryStorageApiStreamingOptions;
import com.google.cloud.teleport.v2.templates.PubSubToBigQuery.Options;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.v2.transforms.ErrorConverters;
import com.google.cloud.teleport.v2.transforms.JSONTransformer;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.v2.utils.BigQueryIOUtils;
import com.google.cloud.teleport.v2.utils.ResourceUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@link PubSubToBigQuery} pipeline is a streaming pipeline which ingests data in JSON format
 * from Cloud Pub/Sub, executes a JSON transformation, and outputs the resulting records to BigQuery. Any errors
 * which occur in the transformation of the data or execution of the UDF will be output to a
 * separate errors table in BigQuery. The errors table will be created if it does not exist prior to
 * execution. Both output and error tables are specified by the user as template parameters.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The Pub/Sub topic exists.
 *   <li>The BigQuery output table exists.
 * </ul>
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/googlecloud-to-googlecloud/README_PubSub_to_BigQuery_Flex.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
        name = "PubSub_to_BigQuery_Flex",
        category = TemplateCategory.STREAMING,
        displayName = "Pub/Sub to BigQuery",
        description =
                "The Pub/Sub to BigQuery template is a streaming pipeline that reads JSON-formatted messages from a Pub/Sub topic or subscription, and writes them to a BigQuery table. "
                        + "You can use the template as a quick solution to move Pub/Sub data to BigQuery. "
                        + "The template reads JSON-formatted messages from Pub/Sub and converts them to BigQuery elements.",
        optionsClass = Options.class,
        flexContainerName = "pubsub-to-bigquery",
        documentation =
                "https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-to-bigquery",
        contactInformation = "https://cloud.google.com/support",
        requirements = {
                "The <a href=\"https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage\">`data` field</a> of Pub/Sub messages must use the JSON format, described in this <a href=\"https://developers.google.com/api-client-library/java/google-http-java-client/json\">JSON guide</a>. For example, messages with values in the `data` field formatted as `{\"k1\":\"v1\", \"k2\":\"v2\"}` can be inserted into a BigQuery table with two columns, named `k1` and `k2`, with a string data type.",
                "The output table must exist prior to running the pipeline. The table schema must match the input JSON objects."
        },
        streaming = true)
public class PubSubToBigQuery {

    /**
     * The log to output status messages to.
     */
    private static final Logger LOG = LoggerFactory.getLogger(PubSubToBigQuery.class);

    /**
     * The tag for the main output for the UDF.
     */
    public static final TupleTag<FailsafeElement<PubsubMessage, String>> UDF_OUT =
            new TupleTag<FailsafeElement<PubsubMessage, String>>() {
            };

    /**
     * The tag for the main output of the json transformation.
     */
    public static final TupleTag<TableRow> TRANSFORM_OUT = new TupleTag<TableRow>() {
    };

    /**
     * The tag for the dead-letter output of the udf.
     */
    public static final TupleTag<FailsafeElement<PubsubMessage, String>> UDF_DEADLETTER_OUT =
            new TupleTag<FailsafeElement<PubsubMessage, String>>() {
            };

    /**
     * The tag for the dead-letter output of the json to table row transform.
     */
    public static final TupleTag<FailsafeElement<PubsubMessage, String>> TRANSFORM_DEADLETTER_OUT =
            new TupleTag<FailsafeElement<PubsubMessage, String>>() {
            };

    /**
     * The default suffix for error tables if dead letter table is not specified.
     */
    public static final String DEFAULT_DEADLETTER_TABLE_SUFFIX = "_error_records";

    public static final String DEFAULT_FILTERED_DEADLETTER_TABLE_SUFFIX = "_filtered_error_records";

    /**
     * Pubsub message/string coder for pipeline.
     */
    public static final FailsafeElementCoder<PubsubMessage, String> CODER =
            FailsafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), StringUtf8Coder.of());

    /**
     * String/String Coder for FailsafeElement.
     */
    public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
            FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    /**
     * The {@link Options} class provides the custom execution options passed by the executor at the
     * command-line.
     */
    public interface Options
            extends PipelineOptions,
            JavascriptTextTransformerOptions,
            BigQueryStorageApiStreamingOptions,
            DataflowPipelineWorkerPoolOptions {

        @TemplateParameter.BigQueryTable(
                order = 1,
                description = "BigQuery output table",
                helpText = "BigQuery table location to write the output to. The table’s schema must match the input JSON objects.")
        String getOutputTableSpec();
        void setOutputTableSpec(String value);

        @TemplateParameter.BigQueryTable(
                order = 6,
                description = "BigQuery filtered output table",
                helpText = "BigQuery table location to write the filtered output to. The table’s schema must match the input JSON objects.")
        String getOutputFilteredTableSpec();
        void setOutputFilteredTableSpec(String value);

        @TemplateParameter.PubsubTopic(
                order = 2,
                optional = true,
                description = "Input Pub/Sub topic",
                helpText = "The Pub/Sub topic to read the input from.")
        String getInputTopic();
        void setInputTopic(String value);

        @TemplateParameter.PubsubSubscription(
                order = 3,
                optional = true,
                description = "Pub/Sub input subscription",
                helpText = "Pub/Sub subscription to read the input from, in the format of 'projects/your-project-id/subscriptions/your-subscription-name'")
        String getInputSubscription();
        void setInputSubscription(String value);

        @TemplateParameter.BigQueryTable(
                order = 4,
                optional = true,
                description = "Table for messages failed to reach the output table (i.e., Deadletter table)",
                helpText = "BigQuery table for failed messages. Messages failed to reach the output table for different reasons (e.g., mismatched schema, malformed json) are written to this table. If it doesn't exist, it will be created during pipeline execution. If not specified, \"outputTableSpec_error_records\" is used instead.")
        String getOutputDeadletterTable();
        void setOutputDeadletterTable(String value);

        @TemplateParameter.BigQueryTable(
                order = 5,
                optional = true,
                description = "Deadletter Table for filtered messages failed to reach the filtered output table (i.e., Deadletter table)",
                helpText = "BigQuery table for failed messages. Messages failed to reach the output table for different reasons (e.g., mismatched schema, malformed json) are written to this table. If it doesn't exist, it will be created during pipeline execution. If not specified, \"outputTableSpec_error_records\" is used instead.")
        String getOutputFilteredDeadletterTable();
        void setOutputFilteredDeadletterTable(String value);

        @TemplateParameter.BigQueryTable(
                order = 7,
                description = "BigQuery table to query QA users",
                helpText = "The BigQuery table to read data from periodically.")
        String getTableToQuery();
        void setTableToQuery(String value);
    }


    /**
     * The main entry-point for pipeline execution. This method will start the pipeline but will not
     * wait for it's execution to finish. If blocking execution is required, use the {@link
     * PubSubToBigQuery#run(Options)} method to start the pipeline and invoke {@code
     * result.waitUntilFinish()} on the {@link PipelineResult}.
     *
     * @param args The command-line args passed by the executor.
     */
    public static void main(String[] args) {
        UncaughtExceptionLogger.register();

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        BigQueryIOUtils.validateBQStorageApiOptionsStreaming(options);

        run(options);
    }

    /**
     * Runs the pipeline to completion with the specified options. This method does not wait until the
     * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
     * object to block until the pipeline is finished running if blocking programmatic execution is
     * required.
     *
     * @param options The execution options.
     * @return The pipeline result.
     */
    public static PipelineResult run(Options options) {

        boolean useInputSubscription = !Strings.isNullOrEmpty(options.getInputSubscription());
        boolean useInputTopic = !Strings.isNullOrEmpty(options.getInputTopic());
        if (useInputSubscription == useInputTopic) {
            throw new IllegalArgumentException(
                    "Either input topic or input subscription must be provided, but not both.");
        }

        Pipeline pipeline = Pipeline.create(options);

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(CODER.getEncodedTypeDescriptor(), CODER);

        PCollection<PubsubMessage> messages = null;
        PCollection<TableRow> qaUsers = null;

        if (useInputSubscription) {
            messages =
                    pipeline.apply(
                            "ReadPubSubSubscription",
                            PubsubIO.readMessagesWithAttributes()
                                    .fromSubscription(options.getInputSubscription()));
        } else {
            messages =
                    pipeline.apply(
                            "ReadPubSubTopic",
                            PubsubIO.readMessagesWithAttributes().fromTopic(options.getInputTopic()));
        }

        // Read from BigQuery table periodically using the parameter
        qaUsers = pipeline
                .apply("ReadFromBigQuery", BigQueryIO.readTableRows()
                        .fromQuery(String.format("SELECT * FROM `%s`", options.getTableToQuery()))
                        .withoutValidation())
                .apply("TriggerPeriodically", Window.<TableRow>into(FixedWindows.of(Duration.standardMinutes(1)))
                        .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(60))))
                        .withAllowedLateness(Duration.ZERO)
                        .discardingFiredPanes());

        // Create a PCollectionView of the filtered rows as a Map
        PCollectionView<Map<String, TableRow>> qaUsersView = qaUsers
                .apply("ExtractUserIds", MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(TableRow.class)))
                        .via(row -> KV.of((String) row.get("user_id"), row)))
                .apply("CreateQAUsersView", View.asMap());

        PCollectionTuple convertedTableRows =
                messages
                        .apply("ConvertMessageToTableRow", new PubsubMessageToTableRow(options));

        PCollection<TableRow> qaFilteredRows = convertedTableRows.get(TRANSFORM_OUT)
                .apply("FilterAndMergeQAUsers", ParDo.of(new FilterAndMergeQAUsersFn(qaUsersView)).withSideInputs(qaUsersView));

        WriteResult filteredWriteResult =
                qaFilteredRows
                        .apply(
                                "WriteSuccessfulQAFilteredRecords",
                                BigQueryIO.writeTableRows()
                                        .withoutValidation()
                                        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                                        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                                        .withExtendedErrorInfo()
                                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                                        .to(options.getOutputFilteredTableSpec()));

        WriteResult writeResult =
                convertedTableRows.get(TRANSFORM_OUT)
                        .apply(
                                "WriteSuccessfulRecords",
                                BigQueryIO.writeTableRows()
                                        .withoutValidation()
                                        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                                        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                                        .withExtendedErrorInfo()
                                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                                        .to(options.getOutputTableSpec()));

        PCollection<FailsafeElement<String, String>> failedInserts =
                BigQueryIOUtils.writeResultToBigQueryInsertErrors(writeResult, options)
                        .apply(
                                "WrapInsertionErrors",
                                MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                                        .via((BigQueryInsertError e) -> wrapBigQueryInsertError(e)))
                        .setCoder(FAILSAFE_ELEMENT_CODER);

        PCollection<FailsafeElement<String, String>> failedFilteredInserts =
                BigQueryIOUtils.writeResultToBigQueryInsertErrors(filteredWriteResult, options)
                        .apply(
                                "WrapQAInsertionErrors",
                                MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                                        .via((BigQueryInsertError e) -> wrapBigQueryInsertError(e)))
                        .setCoder(FAILSAFE_ELEMENT_CODER);

        PCollectionList.of(
                        ImmutableList.of(
                                convertedTableRows.get(UDF_DEADLETTER_OUT),
                                convertedTableRows.get(TRANSFORM_DEADLETTER_OUT)))
                .apply("Flatten", Flatten.pCollections())
                .apply(
                        "WriteFailedRecords",
                        ErrorConverters.WritePubsubMessageErrors.newBuilder()
                                .setErrorRecordsTable(
                                        !Strings.isNullOrEmpty(options.getOutputDeadletterTable())
                                                ? options.getOutputDeadletterTable()
                                                : options.getOutputTableSpec() + DEFAULT_DEADLETTER_TABLE_SUFFIX)
                                .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
                                .build());

        failedInserts.apply(
                "WriteFailedRecords",
                ErrorConverters.WriteStringMessageErrors.newBuilder()
                        .setErrorRecordsTable(
                                !Strings.isNullOrEmpty(options.getOutputDeadletterTable())
                                        ? options.getOutputDeadletterTable()
                                        : options.getOutputTableSpec() + DEFAULT_DEADLETTER_TABLE_SUFFIX)
                        .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
                        .build());

        failedFilteredInserts.apply(
                "WriteQAFilteredFailedRecords",
                ErrorConverters.WriteStringMessageErrors.newBuilder()
                        .setErrorRecordsTable(
                                !Strings.isNullOrEmpty(options.getOutputDeadletterTable())
                                        ? options.getOutputFilteredDeadletterTable()
                                        : options.getOutputFilteredTableSpec() + DEFAULT_FILTERED_DEADLETTER_TABLE_SUFFIX)
                        .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
                        .build());

        return pipeline.run();
    }


    /**
     * The {@link PubsubMessageToTableRow} class is a {@link PTransform} which transforms incoming
     * {@link PubsubMessage} objects into {@link TableRow} objects for insertion into BigQuery while
     * applying an optional UDF to the input. The executions of the UDF and transformation to {@link
     * TableRow} objects is done in a fail-safe way by wrapping the element with it's original payload
     * inside the {@link FailsafeElement} class. The {@link PubsubMessageToTableRow} transform will
     * output a {@link PCollectionTuple} which contains all output and dead-letter {@link
     * PCollection}.
     *
     * <p>The {@link PCollectionTuple} output will contain the following {@link PCollection}:
     *
     * <ul>
     *   <li>{@link PubSubToBigQuery#UDF_OUT} - Contains all {@link FailsafeElement} records
     *       successfully processed by the optional UDF.
     *   <li>{@link PubSubToBigQuery#UDF_DEADLETTER_OUT} - Contains all {@link FailsafeElement}
     *       records which failed processing during the UDF execution.
     *   <li>{@link PubSubToBigQuery#TRANSFORM_OUT} - Contains all records successfully converted from
     *       JSON to {@link TableRow} objects.
     *   <li>{@link PubSubToBigQuery#TRANSFORM_DEADLETTER_OUT} - Contains all {@link FailsafeElement}
     *       records which couldn't be converted to table rows.
     * </ul>
     */
    static class PubsubMessageToTableRow
            extends PTransform<PCollection<PubsubMessage>, PCollectionTuple> {

        private final Options options;

        PubsubMessageToTableRow(Options options) {
            this.options = options;
        }

        @Override
        public PCollectionTuple expand(PCollection<PubsubMessage> input) {

            PCollectionTuple udfOut =
                    input
                            // Map the incoming messages into FailsafeElements so we can recover from failures
                            // across multiple transforms.
                            .apply("MapToRecord", ParDo.of(new PubsubMessageToFailsafeElementFn()))
                            .apply(
                                    "InvokeJSONTransformer",
                                    JSONTransformer.<PubsubMessage>newBuilder()
                                            .setSuccessTag(UDF_OUT)
                                            .setFailureTag(UDF_DEADLETTER_OUT)
                                            .build());

            // Convert the records which were successfully processed by the UDF into TableRow objects.
            PCollectionTuple jsonToTableRowOut =
                    udfOut
                            .get(UDF_OUT)
                            .apply(
                                    "JsonToTableRow",
                                    FailsafeJsonToTableRow.<PubsubMessage>newBuilder()
                                            .setSuccessTag(TRANSFORM_OUT)
                                            .setFailureTag(TRANSFORM_DEADLETTER_OUT)
                                            .build());

            // Re-wrap the PCollections so we can return a single PCollectionTuple
            return PCollectionTuple.of(UDF_OUT, udfOut.get(UDF_OUT))
                    .and(UDF_DEADLETTER_OUT, udfOut.get(UDF_DEADLETTER_OUT))
                    .and(TRANSFORM_OUT, jsonToTableRowOut.get(TRANSFORM_OUT))
                    .and(TRANSFORM_DEADLETTER_OUT, jsonToTableRowOut.get(TRANSFORM_DEADLETTER_OUT));
        }
    }

    /**
     * The {@link PubsubMessageToFailsafeElementFn} wraps an incoming {@link PubsubMessage} with the
     * {@link FailsafeElement} class so errors can be recovered from and the original message can be
     * output to a error records table.
     */
    static class PubsubMessageToFailsafeElementFn
            extends DoFn<PubsubMessage, FailsafeElement<PubsubMessage, String>> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            PubsubMessage message = context.element();
            context.output(
                    FailsafeElement.of(message, new String(message.getPayload(), StandardCharsets.UTF_8)));
        }
    }

    public static class FilterAndMergeQAUsersFn extends DoFn<TableRow, TableRow> implements Serializable {
        private static final long serialVersionUID = 1L;
        private final PCollectionView<Map<String, TableRow>> qaUsersView;

        public FilterAndMergeQAUsersFn(PCollectionView<Map<String, TableRow>> qaUsersView) {
            this.qaUsersView = qaUsersView;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow inputRow = c.element();
            Map<String, TableRow> qaUsers = c.sideInput(qaUsersView);

            // Get the "core" value as a string
            String coreJson = (String) inputRow.get("core");
            if (coreJson != null) {
                // Parse the JSON string into a JsonObject
                JsonObject coreObject = JsonParser.parseString(coreJson).getAsJsonObject();

                // Retrieve the user_id value from the coreObject
                String userId = coreObject.get("user_id").getAsString();

                // Check if the input row's user ID exists in the qaUsers map
                if (qaUsers.containsKey(userId)) {
                    // Create a new TableRow object instead of modifying the input directly
                    TableRow outputRow = new TableRow();
                    outputRow.putAll(inputRow);
                    c.output(outputRow);
                }
            }
        }
    }
}
