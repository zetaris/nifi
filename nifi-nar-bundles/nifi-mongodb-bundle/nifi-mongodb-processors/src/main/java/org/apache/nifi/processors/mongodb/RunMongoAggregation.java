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

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.bson.conversions.Bson;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({"mongo", "aggregation", "aggregate"})
@CapabilityDescription("A processor that runs an aggregation query whenever a flowfile is received.")
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@EventDriven
public class RunMongoAggregation extends AbstractMongoProcessor {

    private final static Set<Relationship> relationships;
    private final static List<PropertyDescriptor> propertyDescriptors;

    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .description("The input flowfile gets sent to this relationship when the query succeeds.")
            .name("original")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .description("The input flowfile gets sent to this relationship when the query fails.")
            .name("failure")
            .build();
    static final Relationship REL_RESULTS = new Relationship.Builder()
            .description("The result set of the aggregation will be sent to this relationship.")
            .name("results")
            .build();

    static final List<Bson> buildAggregationQuery(String query) throws IOException {
        List<Bson> result = new ArrayList<>();

        ObjectMapper mapper = new ObjectMapper();
        List<Map> values = mapper.readValue(query, List.class);
        for (Map val : values) {
            result.add(new BasicDBObject(val));
        }

        return result;
    }

    public static final Validator AGG_VALIDATOR = (subject, value, context) -> {
        final ValidationResult.Builder builder = new ValidationResult.Builder();
        builder.subject(subject).input(value);

        if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
            return builder.valid(true).explanation("Contains Expression Language").build();
        }

        String reason = null;
        try {
            buildAggregationQuery(value);
        } catch (final RuntimeException | IOException e) {
            reason = e.getLocalizedMessage();
        }

        return builder.explanation(reason).valid(reason == null).build();
    };

    static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("mongo-agg-query")
            .displayName("Query")
            .expressionLanguageSupported(true)
            .description("The aggregation query to be executed.")
            .required(true)
            .addValidator(AGG_VALIDATOR)
            .build();

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        _propertyDescriptors.add(CHARSET);
        _propertyDescriptors.add(QUERY);
        _propertyDescriptors.add(QUERY_ATTRIBUTE);
        _propertyDescriptors.add(BATCH_SIZE);
        _propertyDescriptors.add(RESULTS_PER_FLOWFILE);
        _propertyDescriptors.add(SSL_CONTEXT_SERVICE);
        _propertyDescriptors.add(CLIENT_AUTH);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_RESULTS);
        _relationships.add(REL_ORIGINAL);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    static String buildBatch(List batch) {
        ObjectMapper mapper = new ObjectMapper();
        String retVal;
        try {
            retVal = mapper.writeValueAsString(batch.size() > 1 ? batch : batch.get(0));
        } catch (Exception e) {
            retVal = null;
        }

        return retVal;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = null;
        if (context.hasIncomingConnection()) {
            flowFile = session.get();

            if (flowFile == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        String query = context.getProperty(QUERY).evaluateAttributeExpressions(flowFile).getValue();
        String queryAttr = context.getProperty(QUERY_ATTRIBUTE).evaluateAttributeExpressions(flowFile).getValue();
        Integer batchSize = context.getProperty(BATCH_SIZE).asInteger();
        Integer resultsPerFlowfile = context.getProperty(RESULTS_PER_FLOWFILE).asInteger();

        Map attrs = new HashMap();
        if (queryAttr != null && queryAttr.trim().length() > 0) {
            attrs.put(queryAttr, query);
        }

        MongoCollection collection = getCollection(context);
        MongoCursor iter = null;

        try {
            List<Bson> aggQuery = buildAggregationQuery(query);
            AggregateIterable it = collection.aggregate(aggQuery);
            it.batchSize(batchSize != null ? batchSize : 1);

            iter = it.iterator();
            List batch = new ArrayList();

            while (iter.hasNext()) {
                batch.add(iter.next());
                if (batch.size() == resultsPerFlowfile) {
                    writeBatch(buildBatch(batch), flowFile, context, session, attrs, REL_RESULTS);
                    batch = new ArrayList();
                }
            }

            if (batch.size() > 0) {
                writeBatch(buildBatch(batch), flowFile, context, session, attrs, REL_RESULTS);
            }

            if (flowFile != null) {
                session.transfer(flowFile, REL_ORIGINAL);
            }
        } catch (Exception e) {
            getLogger().error("Error running MongoDB aggregation query.", e);
            if (flowFile != null) {
                session.transfer(flowFile, REL_FAILURE);
            }
        } finally {
            if (iter != null) {
                iter.close();
            }
        }
    }
}
