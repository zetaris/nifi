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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.xml.bind.annotation.XmlType;

import com.wordnik.swagger.annotations.ApiModelProperty;

/**
 * DTO for serializing the status of a processor.
 */
@XmlType(name = "processorStatus")
public class ProcessorStatusDTO implements Cloneable {
    private String groupId;
    private String id;
    private String processorName;
    private String processorType;
    private String processorRunStatus;
    private Date statsLastRefreshed;

    private ProcessorStatusSnapshotDTO aggregateStatus;
    private List<NodeProcessorStatusSnapshotDTO> nodeStatuses;

    @ApiModelProperty("The unique ID of the process group that the Processor belongs to")
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    @ApiModelProperty("The unique ID of the Processor")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @ApiModelProperty("The name of the Processor")
    public String getProcessorName() {
        return processorName;
    }

    public void setProcessorName(String processorName) {
        this.processorName = processorName;
    }

    @ApiModelProperty("The type of the Processor")
    public String getProcessorType() {
        return processorType;
    }

    public void setProcessorType(String processorType) {
        this.processorType = processorType;
    }

    @ApiModelProperty("")
    public String getProcessorRunStatus() {
        return processorRunStatus;
    }

    public void setProcessorRunStatus(String processorRunStatus) {
        this.processorRunStatus = processorRunStatus;
    }

    @ApiModelProperty("The timestamp of when the stats were last refreshed")
    public Date getStatsLastRefreshed() {
        return statsLastRefreshed;
    }

    public void setStatsLastRefreshed(Date statsLastRefreshed) {
        this.statsLastRefreshed = statsLastRefreshed;
    }

    @ApiModelProperty("A status snapshot that represents the aggregate stats of all nodes in the cluster. If the NiFi instance is "
        + "a standalone instance, rather than a cluster, this represents the stats of the single instance.")
    public ProcessorStatusSnapshotDTO getAggregateStatus() {
        return aggregateStatus;
    }

    public void setAggregateStatus(ProcessorStatusSnapshotDTO aggregateStatus) {
        this.aggregateStatus = aggregateStatus;
    }

    @ApiModelProperty("A status snapshot for each node in the cluster. If the NiFi instance is a standalone instance, rather than "
        + "a cluster, this may be null.")
    public List<NodeProcessorStatusSnapshotDTO> getNodeStatuses() {
        return nodeStatuses;
    }

    public void setNodeStatuses(List<NodeProcessorStatusSnapshotDTO> nodeStatuses) {
        this.nodeStatuses = nodeStatuses;
    }

    @Override
    public ProcessorStatusDTO clone() {
        final ProcessorStatusDTO other = new ProcessorStatusDTO();
        other.setGroupId(getGroupId());
        other.setId(getId());
        other.setProcessorName(getProcessorName());
        other.setProcessorRunStatus(getProcessorRunStatus());
        other.setProcessorType(getProcessorType());
        other.setStatsLastRefreshed(getStatsLastRefreshed());
        other.setAggregateStatus(getAggregateStatus().clone());

        final List<NodeProcessorStatusSnapshotDTO> nodeStatuses = getNodeStatuses();
        final List<NodeProcessorStatusSnapshotDTO> nodeSnapshots = new ArrayList<>(nodeStatuses.size());
        for (final NodeProcessorStatusSnapshotDTO status : nodeStatuses) {
            nodeSnapshots.add(status.clone());
        }

        return other;
    }
}
