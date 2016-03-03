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
package org.apache.nifi.web.api.dto.status;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.nifi.web.api.dto.util.TimeAdapter;

import com.wordnik.swagger.annotations.ApiModelProperty;

/**
 * DTO for serializing the status history of a single component across the cluster.
 */
@XmlType(name = "statusHistory")
public class StatusHistoryDTO {

    private Date generated;

    private LinkedHashMap<String, String> componentDetails;
    private List<StatusDescriptorDTO> fieldDescriptors;
    private List<StatusSnapshotDTO> aggregateStatusSnapshots;
    private Map<String, List<StatusSnapshotDTO>> nodeStatusSnapshots;


    /**
     * @return when this status history was generated
     */
    @XmlJavaTypeAdapter(TimeAdapter.class)
    @ApiModelProperty("When the status history was generated.")
    public Date getGenerated() {
        return generated;
    }

    public void setGenerated(Date generated) {
        this.generated = generated;
    }

    /**
     * @return key/value pairs that describe the component that the status history belongs to
     */
    @ApiModelProperty("A Map of key/value pairs that describe the component that the status history belongs to")
    public LinkedHashMap<String, String> getComponentDetails() {
        return componentDetails;
    }

    public void setComponentDetails(LinkedHashMap<String, String> componentDetails) {
        this.componentDetails = componentDetails;
    }

    @ApiModelProperty("The Descriptors that provide information on each of the metrics provided in the status history")
    public List<StatusDescriptorDTO> getFieldDescriptors() {
        return fieldDescriptors;
    }

    public void setFieldDescriptors(List<StatusDescriptorDTO> fieldDescriptors) {
        this.fieldDescriptors = fieldDescriptors;
    }

    @ApiModelProperty("A list of StatusSnapshotDTO objects that provide the actual metric values for the component. If the NiFi instance "
        + "is clustered, this will represent the aggregate status across all nodes. If the NiFi instance is not clustered, this will represent "
        + "the status of the entire NiFi instance.")
    public List<StatusSnapshotDTO> getAggregateStatusSnapshots() {
        return aggregateStatusSnapshots;
    }

    public void setAggregateStatusSnapshots(List<StatusSnapshotDTO> aggregateStatusSnapshots) {
        this.aggregateStatusSnapshots = aggregateStatusSnapshots;
    }

    @ApiModelProperty("A Map of Node Address to a list of StatusSnapshotDTO objects that provide the actual metric values for the component, for that node. "
        + "If the NiFi instance is not clustered, this value will be null.")
    public Map<String, List<StatusSnapshotDTO>> getNodeStatusSnapshots() {
        return nodeStatusSnapshots;
    }

    public void setNodeStatusSnapshots(Map<String, List<StatusSnapshotDTO>> nodeStatusSnapshots) {
        this.nodeStatusSnapshots = nodeStatusSnapshots;
    }
}
