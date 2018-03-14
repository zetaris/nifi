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
package org.apache.nifi.processors.azure.storage;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.azure.AbstractAzureBlobProcessor;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;

import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;


@Tags({ "azure", "microsoft", "cloud", "storage", "blob" })
@SeeAlso({ ListAzureBlobStorage.class, FetchAzureBlobStorage.class, PutAzureBlobStorage.class})
@CapabilityDescription("Deletes the provided blob from Azure Storage")
@InputRequirement(Requirement.INPUT_REQUIRED)
public class DeleteAzureBlobStorage extends AbstractAzureBlobProcessor {

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();

        if(flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();
        String containerName = context.getProperty(AzureStorageUtils.CONTAINER).evaluateAttributeExpressions(flowFile).getValue();
        String blobPath = context.getProperty(BLOB).evaluateAttributeExpressions(flowFile).getValue();

        try {
            CloudBlobClient blobClient = AzureStorageUtils.createCloudBlobClient(context, getLogger(), flowFile);
            CloudBlobContainer container = blobClient.getContainerReference(containerName);
            CloudBlob blob = container.getBlockBlobReference(blobPath);
            blob.deleteIfExists();
            session.transfer(flowFile, REL_SUCCESS);

            final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().send(flowFile, blob.getSnapshotQualifiedUri().toString(), transferMillis);
        }catch ( StorageException | URISyntaxException e) {
            getLogger().error("Failed to delete the specified blob {} from Azure Storage. Routing to failure", new Object[]{blobPath}, e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
