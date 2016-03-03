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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.controller.status.RunStatus;
import org.apache.nifi.controller.status.TransmissionStatus;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.web.api.dto.BulletinDTO;

public class StatusMerger {
    public static void merge(final ControllerStatusDTO target, final ControllerStatusDTO toMerge) {
        if (target == null || toMerge == null) {
            return;
        }

        target.setActiveRemotePortCount(target.getActiveRemotePortCount() + toMerge.getActiveRemotePortCount());
        target.setActiveThreadCount(target.getActiveThreadCount() + toMerge.getActiveThreadCount());
        target.setBytesQueued(target.getBytesQueued() + toMerge.getBytesQueued());
        target.setDisabledCount(target.getDisabledCount() + toMerge.getDisabledCount());
        target.setFlowFilesQueued(target.getFlowFilesQueued() + toMerge.getFlowFilesQueued());
        target.setInactiveRemotePortCount(target.getInactiveRemotePortCount() + toMerge.getInactiveRemotePortCount());
        target.setInvalidCount(target.getInvalidCount() + toMerge.getInvalidCount());
        target.setRunningCount(target.getRunningCount() + toMerge.getRunningCount());
        target.setStoppedCount(target.getStoppedCount() + toMerge.getStoppedCount());

        target.setBulletins(mergeBulletins(target.getBulletins(), toMerge.getBulletins()));
        target.setControllerServiceBulletins(mergeBulletins(target.getControllerServiceBulletins(), toMerge.getControllerServiceBulletins()));
        target.setReportingTaskBulletins(mergeBulletins(target.getReportingTaskBulletins(), toMerge.getReportingTaskBulletins()));

        updatePrettyPrintedFields(target);
    }

    public static void updatePrettyPrintedFields(final ControllerStatusDTO target) {
        target.setQueued(prettyPrint(target.getFlowFilesQueued(), target.getBytesQueued()));
        target.setConnectedNodes(formatCount(target.getConnectedNodeCount()) + " / " + formatCount(target.getTotalNodeCount()));
    }

    public static void merge(final ProcessGroupStatusDTO target, final ProcessGroupStatusDTO toMerge) {
        if (target == null || toMerge == null) {
            return;
        }

        target.setBytesIn(target.getBytesIn() + toMerge.getBytesIn());
        target.setFlowFilesIn(target.getFlowFilesIn() + toMerge.getFlowFilesIn());

        target.setBytesQueued(target.getBytesQueued() + toMerge.getBytesQueued());
        target.setFlowFilesQueued(target.getFlowFilesQueued() + toMerge.getFlowFilesQueued());

        target.setBytesRead(target.getBytesRead() + toMerge.getBytesRead());
        target.setBytesWritten(target.getBytesWritten() + toMerge.getBytesWritten());

        target.setBytesOut(target.getBytesOut() + toMerge.getBytesOut());
        target.setFlowFilesOut(target.getFlowFilesOut() + toMerge.getFlowFilesOut());

        target.setBytesTransferred(target.getBytesTransferred() + toMerge.getBytesTransferred());
        target.setFlowFilesTransferred(target.getFlowFilesTransferred() + toMerge.getFlowFilesTransferred());

        target.setBytesReceived(target.getBytesReceived() + toMerge.getBytesReceived());
        target.setFlowFilesReceived(target.getFlowFilesReceived() + toMerge.getFlowFilesReceived());

        target.setBytesSent(target.getBytesSent() + toMerge.getBytesSent());
        target.setFlowFilesSent(target.getFlowFilesSent() + toMerge.getFlowFilesSent());

        target.setActiveThreadCount(target.getActiveThreadCount() + toMerge.getActiveThreadCount());
        target.setBulletins(mergeBulletins(target.getBulletins(), toMerge.getBulletins()));
        updatePrettyPrintedFields(target);

        // connection status
        // sort by id
        final Map<String, ConnectionStatusDTO> mergedConnectionMap = new HashMap<>();
        for (final ConnectionStatusDTO status : replaceNull(target.getConnectionStatus())) {
            mergedConnectionMap.put(status.getId(), status);
        }

        for (final ConnectionStatusDTO statusToMerge : replaceNull(toMerge.getConnectionStatus())) {
            ConnectionStatusDTO merged = mergedConnectionMap.get(statusToMerge.getId());
            if (merged == null) {
                mergedConnectionMap.put(statusToMerge.getId(), statusToMerge.clone());
                continue;
            }

            merge(merged, statusToMerge);
        }
        target.setConnectionStatus(mergedConnectionMap.values());

        // processor status
        final Map<String, ProcessorStatusDTO> mergedProcessorMap = new HashMap<>();
        for (final ProcessorStatusDTO status : replaceNull(target.getProcessorStatus())) {
            mergedProcessorMap.put(status.getId(), status);
        }

        for (final ProcessorStatusDTO statusToMerge : replaceNull(toMerge.getProcessorStatus())) {
            ProcessorStatusDTO merged = mergedProcessorMap.get(statusToMerge.getId());
            if (merged == null) {
                mergedProcessorMap.put(statusToMerge.getId(), statusToMerge.clone());
                continue;
            }

            merge(merged, statusToMerge);
        }
        target.setProcessorStatus(mergedProcessorMap.values());

        // input ports
        final Map<String, PortStatusDTO> mergedInputPortMap = new HashMap<>();
        for (final PortStatusDTO status : replaceNull(target.getInputPortStatus())) {
            mergedInputPortMap.put(status.getId(), status);
        }

        for (final PortStatusDTO statusToMerge : replaceNull(toMerge.getInputPortStatus())) {
            PortStatusDTO merged = mergedInputPortMap.get(statusToMerge.getId());
            if (merged == null) {
                mergedInputPortMap.put(statusToMerge.getId(), statusToMerge.clone());
                continue;
            }

            merge(merged, statusToMerge);
        }
        target.setInputPortStatus(mergedInputPortMap.values());

        // output ports
        final Map<String, PortStatusDTO> mergedOutputPortMap = new HashMap<>();
        for (final PortStatusDTO status : replaceNull(target.getOutputPortStatus())) {
            mergedOutputPortMap.put(status.getId(), status);
        }

        for (final PortStatusDTO statusToMerge : replaceNull(toMerge.getOutputPortStatus())) {
            PortStatusDTO merged = mergedOutputPortMap.get(statusToMerge.getId());
            if (merged == null) {
                mergedOutputPortMap.put(statusToMerge.getId(), statusToMerge.clone());
                continue;
            }

            merge(merged, statusToMerge);
        }
        target.setOutputPortStatus(mergedOutputPortMap.values());

        // child groups
        final Map<String, ProcessGroupStatusDTO> mergedGroupMap = new HashMap<>();
        for (final ProcessGroupStatusDTO status : replaceNull(target.getProcessGroupStatus())) {
            mergedGroupMap.put(status.getId(), status);
        }

        for (final ProcessGroupStatusDTO statusToMerge : replaceNull(toMerge.getProcessGroupStatus())) {
            ProcessGroupStatusDTO merged = mergedGroupMap.get(statusToMerge.getId());
            if (merged == null) {
                mergedGroupMap.put(statusToMerge.getId(), statusToMerge.clone());
                continue;
            }

            merge(merged, statusToMerge);
        }
        target.setOutputPortStatus(mergedOutputPortMap.values());

        // remote groups
        final Map<String, RemoteProcessGroupStatusDTO> mergedRemoteGroupMap = new HashMap<>();
        for (final RemoteProcessGroupStatusDTO status : replaceNull(target.getRemoteProcessGroupStatus())) {
            mergedRemoteGroupMap.put(status.getId(), status);
        }

        for (final RemoteProcessGroupStatusDTO statusToMerge : replaceNull(toMerge.getRemoteProcessGroupStatus())) {
            RemoteProcessGroupStatusDTO merged = mergedRemoteGroupMap.get(statusToMerge.getId());
            if (merged == null) {
                mergedRemoteGroupMap.put(statusToMerge.getId(), statusToMerge.clone());
                continue;
            }

            merge(merged, statusToMerge);
        }
        target.setRemoteProcessGroupStatus(mergedRemoteGroupMap.values());
    }

    private static <T> Collection<T> replaceNull(final Collection<T> collection) {
        return (collection == null) ? Collections.<T> emptyList() : collection;
    }

    public static List<BulletinDTO> mergeBulletins(final List<BulletinDTO> targetBulletins, final List<BulletinDTO> toMerge) {
        final List<BulletinDTO> bulletins = new ArrayList<>();
        if (targetBulletins != null) {
            bulletins.addAll(targetBulletins);
        }

        if (toMerge != null) {
            bulletins.addAll(toMerge);
        }

        return bulletins;
    }

    /**
     * Updates the fields that are "pretty printed" based on the raw values currently set. For example,
     * {@link ProcessGroupStatusDTO#setInput(String)} will be called with the pretty-printed form of the
     * FlowFile counts and sizes retrieved via {@link ProcessGroupStatusDTO#getFlowFilesIn()} and
     * {@link ProcessGroupStatusDTO#getBytesIn()}.
     *
     * This logic is performed here, rather than in the DTO itself because the DTO needs to be kept purely
     * getters & setters - otherwise the automatic marshalling and unmarshalling to/from JSON becomes very
     * complicated.
     *
     * @param target the DTO to update
     */
    public static void updatePrettyPrintedFields(final ProcessGroupStatusDTO target) {
        target.setQueued(prettyPrint(target.getFlowFilesQueued(), target.getBytesQueued()));
        target.setQueuedCount(formatCount(target.getFlowFilesQueued()));
        target.setQueuedSize(formatDataSize(target.getBytesQueued()));
        target.setInput(prettyPrint(target.getFlowFilesIn(), target.getBytesIn()));
        target.setRead(formatDataSize(target.getBytesRead()));
        target.setWritten(formatDataSize(target.getBytesWritten()));
        target.setOutput(prettyPrint(target.getFlowFilesOut(), target.getBytesOut()));
        target.setTransferred(prettyPrint(target.getFlowFilesTransferred(), target.getBytesTransferred()));
        target.setReceived(prettyPrint(target.getFlowFilesReceived(), target.getBytesReceived()));
        target.setSent(prettyPrint(target.getFlowFilesSent(), target.getBytesSent()));
    }


    public static void merge(final ProcessorStatusDTO target, final ProcessorStatusDTO toMerge) {
        if (target == null || toMerge == null) {
            return;
        }

        // if the status to merge is invalid allow it to take precedence. whether the
        // processor run status is disabled/stopped/running is part of the flow configuration
        // and should not differ amongst nodes. however, whether a processor is invalid
        // can be driven by environmental conditions. this check allows any of those to
        // take precedence over the configured run status.
        if (RunStatus.Invalid.name().equals(toMerge.getRunStatus())) {
            target.setRunStatus(RunStatus.Invalid.name());
        }

        target.setBytesRead(target.getBytesRead() + toMerge.getBytesRead());
        target.setBytesWritten(target.getBytesWritten() + toMerge.getBytesWritten());
        target.setFlowFilesIn(target.getFlowFilesIn() + toMerge.getFlowFilesIn());
        target.setBytesIn(target.getBytesIn() + toMerge.getBytesIn());
        target.setFlowFilesOut(target.getFlowFilesOut() + toMerge.getFlowFilesOut());
        target.setBytesOut(target.getBytesOut() + toMerge.getBytesOut());
        target.setTaskCount(target.getTaskCount() + toMerge.getTaskCount());
        target.setTaskDuration(target.getTaskDuration() + toMerge.getTaskDuration());
        target.setActiveThreadCount(target.getActiveThreadCount() + toMerge.getActiveThreadCount());
        target.setBulletins(mergeBulletins(target.getBulletins(), toMerge.getBulletins()));
        updatePrettyPrintedFields(target);
    }

    public static void updatePrettyPrintedFields(final ProcessorStatusDTO target) {
        target.setInput(prettyPrint(target.getFlowFilesIn(), target.getBytesIn()));
        target.setRead(formatDataSize(target.getBytesRead()));
        target.setWritten(formatDataSize(target.getBytesWritten()));
        target.setOutput(prettyPrint(target.getFlowFilesOut(), target.getBytesOut()));

        final Integer taskCount = target.getTaskCount();
        final String tasks = (taskCount == null) ? "-" : formatCount(taskCount);
        target.setTasks(tasks);

        target.setTasksDuration(FormatUtils.formatHoursMinutesSeconds(target.getTaskDuration(), TimeUnit.NANOSECONDS));
    }


    public static void merge(final ConnectionStatusDTO target, final ConnectionStatusDTO toMerge) {
        if (target == null || toMerge == null) {
            return;
        }

        target.setFlowFilesIn(target.getFlowFilesIn() + toMerge.getFlowFilesIn());
        target.setBytesIn(target.getBytesIn() + toMerge.getBytesIn());
        target.setFlowFilesOut(target.getFlowFilesOut() + toMerge.getFlowFilesOut());
        target.setBytesOut(target.getBytesOut() + toMerge.getBytesOut());
        target.setFlowFilesQueued(target.getFlowFilesQueued() + toMerge.getFlowFilesQueued());
        target.setBytesQueued(target.getBytesQueued() + toMerge.getBytesQueued());
        updatePrettyPrintedFields(target);
    }

    public static void updatePrettyPrintedFields(final ConnectionStatusDTO target) {
        target.setQueued(prettyPrint(target.getFlowFilesQueued(), target.getBytesQueued()));
        target.setQueuedCount(formatCount(target.getFlowFilesQueued()));
        target.setQueuedSize(formatDataSize(target.getBytesQueued()));
        target.setInput(prettyPrint(target.getFlowFilesIn(), target.getBytesIn()));
        target.setOutput(prettyPrint(target.getFlowFilesOut(), target.getBytesOut()));
    }



    public static void merge(final RemoteProcessGroupStatusDTO target, final RemoteProcessGroupStatusDTO toMerge) {
        final String transmittingValue = TransmissionStatus.Transmitting.name();
        if (transmittingValue.equals(target.getTransmissionStatus()) || transmittingValue.equals(toMerge.getTransmissionStatus())) {
            target.setTransmissionStatus(transmittingValue);
        }

        target.setActiveThreadCount(target.getActiveThreadCount() + toMerge.getActiveThreadCount());

        final List<String> authIssues = new ArrayList<>();
        if (target.getAuthorizationIssues() != null) {
            authIssues.addAll(target.getAuthorizationIssues());
        }
        if (toMerge.getAuthorizationIssues() != null) {
            authIssues.addAll(toMerge.getAuthorizationIssues());
        }
        target.setAuthorizationIssues(authIssues);

        target.setFlowFilesSent(target.getFlowFilesSent() + toMerge.getFlowFilesSent());
        target.setBytesSent(target.getBytesSent() + toMerge.getBytesSent());
        target.setFlowFilesReceived(target.getFlowFilesReceived() + toMerge.getFlowFilesReceived());
        target.setBytesReceived(target.getBytesReceived() + toMerge.getBytesReceived());
        target.setBulletins(mergeBulletins(target.getBulletins(), toMerge.getBulletins()));
        updatePrettyPrintedFields(target);
    }

    public static void updatePrettyPrintedFields(final RemoteProcessGroupStatusDTO target) {
        target.setReceived(prettyPrint(target.getFlowFilesReceived(), target.getBytesReceived()));
        target.setSent(prettyPrint(target.getFlowFilesSent(), target.getBytesSent()));
    }



    public static void merge(final PortStatusDTO target, final PortStatusDTO toMerge) {
        if (target == null || toMerge == null) {
            return;
        }

        target.setActiveThreadCount(target.getActiveThreadCount() + toMerge.getActiveThreadCount());
        target.setFlowFilesIn(target.getFlowFilesIn() + toMerge.getFlowFilesIn());
        target.setBytesIn(target.getBytesIn() + toMerge.getBytesIn());
        target.setFlowFilesOut(target.getFlowFilesOut() + toMerge.getFlowFilesOut());
        target.setBytesOut(target.getBytesOut() + toMerge.getBytesOut());
        target.setTransmitting(Boolean.TRUE.equals(target.isTransmitting()) || Boolean.TRUE.equals(toMerge.isTransmitting()));

        // should be unnecessary here since ports run status not should be affected by
        // environmental conditions but doing so in case that changes
        if (RunStatus.Invalid.name().equals(toMerge.getRunStatus())) {
            target.setRunStatus(RunStatus.Invalid.name());
        }

        target.setBulletins(mergeBulletins(target.getBulletins(), toMerge.getBulletins()));
        updatePrettyPrintedFields(target);
    }

    public static void updatePrettyPrintedFields(final PortStatusDTO target) {
        target.setInput(prettyPrint(target.getFlowFilesIn(), target.getBytesIn()));
        target.setOutput(prettyPrint(target.getFlowFilesOut(), target.getBytesOut()));
    }


    public static String formatCount(final Integer intStatus) {
        return intStatus == null ? "-" : FormatUtils.formatCount(intStatus);
    }

    public static String formatDataSize(final Long longStatus) {
        return longStatus == null ? "-" : FormatUtils.formatDataSize(longStatus);
    }

    public static String prettyPrint(final Integer count, final Long bytes) {
        return formatCount(count) + " / " + formatDataSize(bytes);
    }

}
