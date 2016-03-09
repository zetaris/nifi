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

import java.util.List;

import javax.xml.bind.annotation.XmlType;

import com.wordnik.swagger.annotations.ApiModelProperty;

@XmlType(name = "processGroupStatus")
public class ProcessGroupStatusDTO implements Cloneable {
    private String id;
    private String name;

    private ProcessGroupStatusSnapshotDTO aggregateStatus;
    private List<NodeProcessGroupStatusSnapshotDTO> nodeStatuses;

    @ApiModelProperty("The ID of the Process Group")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @ApiModelProperty("The name of the PRocess Group")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @ApiModelProperty("The aggregate status of all nodes in the cluster")
    public ProcessGroupStatusSnapshotDTO getAggregateStatus() {
        return aggregateStatus;
    }

    public void setAggregateStatus(ProcessGroupStatusSnapshotDTO aggregateStatus) {
        this.aggregateStatus = aggregateStatus;
    }

    @ApiModelProperty("The status reported by each node in the cluster. If the NiFi instance is a standalone instance, rather than "
        + "a clustered instance, this value may be null.")
    public List<NodeProcessGroupStatusSnapshotDTO> getNodeStatuses() {
        return nodeStatuses;
    }

    public void setNodeStatuses(List<NodeProcessGroupStatusSnapshotDTO> nodeStatuses) {
        this.nodeStatuses = nodeStatuses;
    }
}
