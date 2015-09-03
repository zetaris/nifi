/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.avro;

import com.sun.corba.se.spi.ior.ObjectKey;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.util.ObjectHolder;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

@SideEffectFree
@SupportsBatching
@Tags({ "avro", "split" })
@CapabilityDescription("Splits a binary encoded Avro datafile into smaller units based on the configuration. If Split Size " +
        "is set to 1, Output Type to Datafile, and Output Encoding to Binary, then the original file will be split into one " +
        "binary datafile per record in the original file.")
public class SplitAvro extends AbstractProcessor {

    public static final String BLOCK_SPLIT_VALUE = "Block";
    public static final String RECORD_SPLIT_VALUE = "Record";

    public static final AllowableValue BLOCK_SPLIT = new AllowableValue(BLOCK_SPLIT_VALUE, BLOCK_SPLIT_VALUE, "Split at Avro block boundaries");
    public static final AllowableValue RECORD_SPLIT = new AllowableValue(RECORD_SPLIT_VALUE, RECORD_SPLIT_VALUE, "Split at Record boundaries");

    public static final PropertyDescriptor SPLIT_STRATEGY = new PropertyDescriptor.Builder()
            .name("Split Strategy")
            .description("The strategy for splitting the incoming datafile. Splitting on records will cause each record to be " +
                    "read and de-serialized, where as splitting on blocks can bypass deserializing, but can only approximate the " +
                    "number of records per split.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .allowableValues(BLOCK_SPLIT, RECORD_SPLIT)
            .defaultValue(RECORD_SPLIT.getValue())
            .build();

    public static final PropertyDescriptor SPLIT_SIZE = new PropertyDescriptor.Builder()
            .name("Split Size")
            .description("The number of Avro records to include per split. If Split Strategy is set to Block, this will be " +
                    "an approximation.")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .required(true)
            .defaultValue("1")
            .build();

    public static final AllowableValue DATAFILE_OUTPUT = new AllowableValue("Datafile", "Datafile", "Avro's object container file format");
    public static final AllowableValue RECORD_OUTPUT = new AllowableValue("Record", "Record", "Bare Avro records");

    public static final PropertyDescriptor OUTPUT_STRATEGY = new PropertyDescriptor.Builder()
            .name("Output Strategy")
            .description("The number of Avro records to include per split file.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .allowableValues(DATAFILE_OUTPUT, RECORD_OUTPUT)
            .defaultValue(DATAFILE_OUTPUT.getValue())
            .build();

    public static final AllowableValue BINARY_ENCODING = new AllowableValue("Binary");
    public static final AllowableValue JSON_ENCODING = new AllowableValue("JSON");

    public static final PropertyDescriptor OUTPUT_ENCODING = new PropertyDescriptor.Builder()
            .name("Output Encoding")
            .description("The Avro encoding for each record in the output.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .allowableValues(BINARY_ENCODING, JSON_ENCODING)
            .defaultValue(BINARY_ENCODING.getValue())
            .build();


    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile that was split. If the FlowFile fails processing, nothing will be sent to " +
                    "this relationship")
            .build();
    public static final Relationship REL_SPLIT = new Relationship.Builder()
            .name("split")
            .description("All new files split from the original FlowFile will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile fails processing for any reason (for example, the FlowFile is not valid Avro), " +
                    "it will be routed to this relationship")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SPLIT_STRATEGY);
        properties.add(SPLIT_SIZE);
        properties.add(OUTPUT_STRATEGY);
        properties.add(OUTPUT_ENCODING);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_SPLIT);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String splitStrategy = context.getProperty(SPLIT_STRATEGY).getValue();
        final int splitSize = context.getProperty(SPLIT_SIZE).asInteger();

        Splitter splitter;
        switch (splitStrategy) {
            case RECORD_SPLIT_VALUE:
                splitter = new RecordSplitter(splitSize);
                break;
            case BLOCK_SPLIT_VALUE:
                splitter = new BlockSplitter(splitSize);
                break;
            default:
                throw new AssertionError();
        }

        try {
            final List<FlowFile> splits = splitter.split(session, flowFile);
            session.transfer(splits, REL_SPLIT);
            session.transfer(flowFile, REL_ORIGINAL);
        } catch (IOException e) {
            getLogger().error("Failed to split {} due to {}", new Object[] {flowFile, e.getMessage()}, e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    /**
     * Able to split an incoming Avro datafile into multiple smaller outputs.
     */
    private interface Splitter {
        List<FlowFile> split(final ProcessSession session, final FlowFile originalFlowFile) throws IOException;
    }

    /**
     * Splits the incoming Avro datafile into batches of records by reading and de-serializing each record.
     */
    private class RecordSplitter implements Splitter {

        private final int splitSize;

        public RecordSplitter(final int splitSize) {
            this.splitSize = splitSize;
        }

        @Override
        public List<FlowFile> split(final ProcessSession session, final FlowFile originalFlowFile) throws IOException {
            final List<FlowFile> childFlowFiles = new ArrayList<>();
            // keep this holder out here so we can reuse a single GenericRecord instance across all reads
            final ObjectHolder<GenericRecord> recordHolder = new ObjectHolder<>(null);

            session.read(originalFlowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream rawIn) throws IOException {
                    try (final InputStream in = new BufferedInputStream(rawIn);
                         final DataFileStream<GenericRecord> reader = new DataFileStream<>(in, new GenericDatumReader<GenericRecord>())) {

                        final ObjectHolder<String> codec = new ObjectHolder<>(reader.getMetaString(DataFileConstants.CODEC));
                        if (codec.get() == null) {
                            codec.set(DataFileConstants.NULL_CODEC);
                        }

                        while (reader.hasNext()) {
                            FlowFile childFlowFile = session.create(originalFlowFile);
                            childFlowFile = session.write(childFlowFile, new OutputStreamCallback() {
                                @Override
                                public void process(OutputStream rawOut) throws IOException {
                                    try (final BufferedOutputStream out = new BufferedOutputStream(rawOut);
                                         final DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>())) {

                                        writer.setCodec(CodecFactory.fromString(codec.get()));
                                        writer.create(reader.getSchema(), out);

                                        // append to the current FlowFile until no more records or splitSize is reached
                                        int recordCount = 0;
                                        while (reader.hasNext() && recordCount < splitSize) {
                                            recordHolder.set(reader.next(recordHolder.get()));
                                            writer.append(recordHolder.get());
                                            recordCount++;
                                        }
                                        writer.flush();
                                    }
                                }
                            });
                            childFlowFiles.add(childFlowFile);
                        }
                    }
                }
            });

            return childFlowFiles;
        }
    }

    /**
     * Splits the incoming Avro datafile on block boundaries and can bypass de-serializing records, the split sizes will
     * approximate as it will split on the next block boundary that contains enough records to pass the split size.
     */
    private class BlockSplitter implements Splitter {

        private final int splitSize;

        public BlockSplitter(final int splitSize) {
            this.splitSize = splitSize;
        }

        @Override
        public List<FlowFile> split(final ProcessSession session, final FlowFile originalFlowFile) throws IOException {
            final List<FlowFile> childFlowFiles = new ArrayList<>();

            session.read(originalFlowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream rawIn) throws IOException {
                    try (final InputStream in = new BufferedInputStream(rawIn);
                         final DataFileStream<GenericRecord> reader = new DataFileStream<>(in, new GenericDatumReader<GenericRecord>())) {

                        final ObjectHolder<String> codec = new ObjectHolder<>(reader.getMetaString(DataFileConstants.CODEC));
                        if (codec.get() == null) {
                            codec.set(DataFileConstants.NULL_CODEC);
                        }

                        final ObjectHolder<ByteBuffer> outerBlock = new ObjectHolder<>(reader.nextBlock());
                        while (outerBlock.get() != null) {
                            FlowFile childFlowFile = session.create(originalFlowFile);
                            childFlowFile = session.write(childFlowFile, new OutputStreamCallback() {
                                @Override
                                public void process(OutputStream rawOut) throws IOException {
                                    try (final BufferedOutputStream out = new BufferedOutputStream(rawOut);
                                         final DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>())) {

                                        writer.setCodec(CodecFactory.fromString(codec.get()));
                                        writer.create(reader.getSchema(), out);
                                        writer.sync();

                                        // we already read the first block so update the count and append that block
                                        long recordCount = reader.getBlockCount();
                                        //writer.appendEncoded(outerBlock.get());
                                        out.write(outerBlock.get().array());
                                        writer.sync();

                                        if (recordCount < splitSize) {
                                            try {
                                                // now loop again until no more blocks or we reached the split size
                                                final ObjectHolder<ByteBuffer> innerBlock = new ObjectHolder<>(reader.nextBlock());
                                                while (innerBlock.get() != null && recordCount < splitSize) {
                                                    //writer.appendEncoded(innerBlock.get());
                                                    out.write(innerBlock.get().array());
                                                    writer.sync();

                                                    recordCount += reader.getBlockCount();
                                                    innerBlock.set(reader.nextBlock());
                                                }

                                                if (innerBlock.get() != null) {
                                                    //writer.appendEncoded(innerBlock.get());
                                                    out.write(innerBlock.get().array());
                                                    writer.sync();
                                                }
                                            } catch (NoSuchElementException e) {
                                                getLogger().debug("Reached end of datafile for {}", new Object[]{originalFlowFile});
                                            }
                                        }

                                        writer.flush();
                                    }
                                }
                            });

                            childFlowFiles.add(childFlowFile);
                            outerBlock.set(reader.nextBlock());
                        }
                    } catch (NoSuchElementException e) {
                        getLogger().debug("Reached end of datafile for {}", new Object[] {originalFlowFile});
                    }
                }
            });

            return childFlowFiles;
        }
    }

}
