/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.nifi.processors.mongodb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RunMongoAggregationIT {

    private static final String MONGO_URI = "mongodb://localhost";
    private static final String DB_NAME   = String.format("agg_test-%s", Calendar.getInstance().getTimeInMillis());
    private static final String COLLECTION_NAME = "agg_test_data";
    private static final String AGG_ATTR = "mongo.aggregation.query";

    private TestRunner runner;
    private MongoClient mongoClient;
    private Map<String, Integer> mappings;

    @Before
    public void setup() {
        runner = TestRunners.newTestRunner(RunMongoAggregation.class);
        runner.setVariable("uri", MONGO_URI);
        runner.setVariable("db", DB_NAME);
        runner.setVariable("collection", COLLECTION_NAME);
        runner.setProperty(AbstractMongoProcessor.URI, "${uri}");
        runner.setProperty(AbstractMongoProcessor.DATABASE_NAME, "${db}");
        runner.setProperty(AbstractMongoProcessor.COLLECTION_NAME, "${collection}");
        runner.setProperty(RunMongoAggregation.QUERY_ATTRIBUTE, AGG_ATTR);
        runner.setValidateExpressionUsage(true);

        mongoClient = new MongoClient(new MongoClientURI(MONGO_URI));

        MongoCollection<Document> collection = mongoClient.getDatabase(DB_NAME).getCollection(COLLECTION_NAME);
        String[] values = new String[] { "a", "b", "c" };
        mappings = new HashMap<>();

        for (int x = 0; x < values.length; x++) {
            for (int y = 0; y < x + 2; y++) {
                Document doc = new Document().append("val", values[x]);
                collection.insertOne(doc);
            }
            mappings.put(values[x], x + 2);
        }
    }

    @After
    public void teardown() {
        runner = null;

        mongoClient.getDatabase(DB_NAME).drop();
    }

    @Test
    public void testAggregation() throws Exception {
        final String queryInput = "[\n" +
                "    {\n" +
                "        \"$project\": {\n" +
                "            \"_id\": 0,\n" +
                "            \"val\": 1\n" +
                "        }\n" +
                "    },\n" +
                "    {\n" +
                "        \"$group\": {\n" +
                "            \"_id\": \"$val\",\n" +
                "            \"doc_count\": {\n" +
                "                \"$sum\": 1\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "]";
        runner.setProperty(RunMongoAggregation.QUERY, queryInput);
        runner.enqueue("test");
        runner.run(1, true, true);

        evaluateRunner(1);

        runner.clearTransferState();

        runner.setIncomingConnection(false);
        runner.run(); //Null parent flowfile
        evaluateRunner(0);

        runner.run();
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(RunMongoAggregation.REL_RESULTS);
        for (MockFlowFile mff : flowFiles) {
            String val = mff.getAttribute(AGG_ATTR);
            Assert.assertNotNull("Missing query attribute", val);
            Assert.assertEquals("Value was wrong", val, queryInput);
        }
    }

    @Test
    public void testExpressionLanguageSupport() throws Exception {
        runner.setVariable("fieldName", "$val");
        runner.setProperty(RunMongoAggregation.QUERY, "[\n" +
                "    {\n" +
                "        \"$project\": {\n" +
                "            \"_id\": 0,\n" +
                "            \"val\": 1\n" +
                "        }\n" +
                "    },\n" +
                "    {\n" +
                "        \"$group\": {\n" +
                "            \"_id\": \"${fieldName}\",\n" +
                "            \"doc_count\": {\n" +
                "                \"$sum\": 1\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "]");

        runner.enqueue("test");
        runner.run(1, true, true);
        evaluateRunner(1);
    }

    @Test
    public void testInvalidQuery(){
        runner.setProperty(RunMongoAggregation.QUERY, "[\n" +
            "    {\n" +
                "        \"$invalid_stage\": {\n" +
                "            \"_id\": 0,\n" +
                "            \"val\": 1\n" +
                "        }\n" +
                "    }\n" +
            "]"
        );
        runner.enqueue("test");
        runner.run(1, true, true);
        runner.assertTransferCount(RunMongoAggregation.REL_RESULTS, 0);
        runner.assertTransferCount(RunMongoAggregation.REL_ORIGINAL, 0);
        runner.assertTransferCount(RunMongoAggregation.REL_FAILURE, 1);
    }

    private void evaluateRunner(int original) throws IOException {
        runner.assertTransferCount(RunMongoAggregation.REL_RESULTS, mappings.size());
        runner.assertTransferCount(RunMongoAggregation.REL_ORIGINAL, original);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(RunMongoAggregation.REL_RESULTS);
        ObjectMapper mapper = new ObjectMapper();
        for (MockFlowFile mockFlowFile : flowFiles) {
            byte[] raw = runner.getContentAsByteArray(mockFlowFile);
            Map read = mapper.readValue(raw, Map.class);
            Assert.assertTrue("Value was not found", mappings.containsKey(read.get("_id")));

            String queryAttr = mockFlowFile.getAttribute(AGG_ATTR);
            Assert.assertNotNull("Query attribute was null.", queryAttr);
            Assert.assertTrue("Missing $project", queryAttr.contains("$project"));
            Assert.assertTrue("Missing $group", queryAttr.contains("$group"));
        }
    }
}
