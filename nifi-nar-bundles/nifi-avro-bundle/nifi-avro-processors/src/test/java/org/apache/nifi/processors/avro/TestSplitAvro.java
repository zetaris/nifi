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

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestSplitAvro {

    private ByteArrayOutputStream users;

    @Before
    public void setup() throws IOException {
        this.users = new ByteArrayOutputStream();

        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/user.avsc"));

        final List<GenericRecord> userList = new ArrayList<>();
        for (int i=0; i < 100; i++) {
            final GenericRecord user = new GenericData.Record(schema);
            user.put("name", "name" + i);
            user.put("favorite_number", i);
            userList.add(user);
        }

        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(schema))) {
            dataFileWriter.create(schema, users);
            for (GenericRecord user : userList) {
                dataFileWriter.append(user);
            }
            dataFileWriter.flush();
        }
    }

    @Test
    public void testRecordSplitSingleRecord() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitAvro());

        runner.enqueue(users.toByteArray());
        runner.run();

        runner.assertTransferCount(SplitAvro.REL_SPLIT, 100);
        runner.assertTransferCount(SplitAvro.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitAvro.REL_FAILURE, 0);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SplitAvro.REL_SPLIT);
        checkFlowFiles(flowFiles, 1);
    }

    @Test
    public void testRecordSplitMultipleRecordsEvenSplit() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitAvro());
        runner.setProperty(SplitAvro.SPLIT_SIZE, "20");

        runner.enqueue(users.toByteArray());
        runner.run();

        runner.assertTransferCount(SplitAvro.REL_SPLIT, 5);
        runner.assertTransferCount(SplitAvro.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitAvro.REL_FAILURE, 0);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SplitAvro.REL_SPLIT);
        checkFlowFiles(flowFiles, 20);
    }

    @Test
    public void testRecordSplitWithSplitSizeLarger() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitAvro());
        runner.setProperty(SplitAvro.SPLIT_SIZE, "200");

        runner.enqueue(users.toByteArray());
        runner.run();

        runner.assertTransferCount(SplitAvro.REL_SPLIT, 1);
        runner.assertTransferCount(SplitAvro.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitAvro.REL_FAILURE, 0);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SplitAvro.REL_SPLIT);
        checkFlowFiles(flowFiles, 100);
    }

    @Test
    public void testBlockSplitWithSplitSizeLarger() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitAvro());
        runner.setProperty(SplitAvro.SPLIT_STRATEGY, SplitAvro.BLOCK_SPLIT_VALUE);
        runner.setProperty(SplitAvro.SPLIT_SIZE, "200");

        runner.enqueue(users.toByteArray());
        runner.run();

        runner.assertTransferCount(SplitAvro.REL_SPLIT, 1);
        runner.assertTransferCount(SplitAvro.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitAvro.REL_FAILURE, 0);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SplitAvro.REL_SPLIT);
        checkFlowFiles(flowFiles, 100);
    }

    private void checkFlowFiles(List<MockFlowFile> flowFiles, int splitSize) throws IOException {
        for (final MockFlowFile flowFile : flowFiles) {
            String content = new String(flowFile.toByteArray());
            System.out.println(content);

            try (final ByteArrayInputStream in = new ByteArrayInputStream(flowFile.toByteArray());
                final DataFileStream<GenericRecord> reader = new DataFileStream<>(in, new GenericDatumReader<GenericRecord>())) {

                int count = 0;
                GenericRecord record = null;
                while (reader.hasNext()) {
                    record = reader.next(record);
                    Assert.assertNotNull(record.get("name"));
                    Assert.assertNotNull(record.get("favorite_number"));
                    count++;
                    System.out.println("count = " + count);
                }
                Assert.assertEquals(splitSize, count);
            }
        }
    }

}
