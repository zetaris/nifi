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
package org.apache.nifi.controller.repository.claim;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.events.EventReporter;
import org.apache.nifi.reporting.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandardContentClaimManager implements ContentClaimManager {

    private static final ConcurrentMap<ContentClaim, AtomicInteger> claimantCounts = new ConcurrentHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(StandardContentClaimManager.class);

    private static final ConcurrentMap<String, BlockingQueue<ContentClaim>> destructableClaims = new ConcurrentHashMap<>();
    private final EventReporter eventReporter;
    private final AtomicLong lastBulletinTime = new AtomicLong(0L);

    public StandardContentClaimManager(final EventReporter eventReporter) {
        this.eventReporter = eventReporter;
    }

    @Override
    public ContentClaim newContentClaim(final String container, final String section, final String id, final boolean lossTolerant) {
        return new StandardContentClaim(container, section, id, lossTolerant);
    }

    private static AtomicInteger getCounter(final ContentClaim claim) {
        if (claim == null) {
            return null;
        }

        AtomicInteger counter = claimantCounts.get(claim);
        if (counter != null) {
            return counter;
        }

        counter = new AtomicInteger(0);
        final AtomicInteger existingCounter = claimantCounts.putIfAbsent(claim, counter);
        return existingCounter == null ? counter : existingCounter;
    }

    @Override
    public int getClaimantCount(final ContentClaim claim) {
        if (claim == null) {
            return 0;
        }
        final AtomicInteger counter = claimantCounts.get(claim);
        return counter == null ? 0 : counter.get();
    }

    @Override
    public int decrementClaimantCount(final ContentClaim claim) {
        if (claim == null) {
            return 0;
        }

        final AtomicInteger counter = claimantCounts.get(claim);
        if (counter == null) {
            logger.debug("Decrementing claimant count for {} but claimant count is not known. Returning -1", claim);
            return -1;
        }

        final int newClaimantCount = counter.decrementAndGet();
        logger.debug("Decrementing claimant count for {} to {}", claim, newClaimantCount);
        if (newClaimantCount == 0) {
            claimantCounts.remove(claim);
        }
        return newClaimantCount;
    }

    @Override
    public int incrementClaimantCount(final ContentClaim claim) {
        return incrementClaimantCount(claim, false);
    }

    @Override
    public int incrementClaimantCount(final ContentClaim claim, final boolean newClaim) {
        final AtomicInteger counter = getCounter(claim);

        final int newClaimantCount = counter.incrementAndGet();
        logger.debug("Incrementing claimant count for {} to {}", claim, newClaimantCount);
        // If the claimant count moved from 0 to 1, remove it from the queue of destructable claims.
        if (!newClaim && newClaimantCount == 1) {
            destructableClaims.remove(claim);
        }
        return newClaimantCount;
    }

    @Override
    public void markDestructable(final ContentClaim claim) {
        if (claim == null) {
            return;
        }

        if (getClaimantCount(claim) > 0) {
            return;
        }

        logger.debug("Marking claim {} as destructable", claim);
        try {
            final BlockingQueue<ContentClaim> destructableQueue = getDestructableClaimQueue(claim.getContainer());
            final boolean accepted = destructableQueue.offer(claim);
            if (!accepted) {
                while (!destructableQueue.offer(claim, 30, TimeUnit.MINUTES)) {
                }

                // If it's been 5+ minutes since we emitted a bulletin, emit a bulletin notifying user that there is backpressure 
                final long lastTimestamp = lastBulletinTime.get();
                final long now = System.currentTimeMillis();
                if (now - lastTimestamp > TimeUnit.MINUTES.toMillis(5)) {
                    final boolean emitBulletin = lastBulletinTime.compareAndSet(lastTimestamp, now);
                    
                    if (emitBulletin) {
                        eventReporter.reportEvent(Severity.WARNING, "Content Repository", "The Content Repository is unable to destroy content as fast "
                            + "as it is being created. The flow will be slowed in order to adjust for this.");
                    }
                }
            }
        } catch (final InterruptedException ie) {
        }
    }

    private BlockingQueue<ContentClaim> getDestructableClaimQueue(final String container) {
        BlockingQueue<ContentClaim> claimQueue = destructableClaims.get(container);
        if (claimQueue == null) {
            claimQueue = new LinkedBlockingQueue<>(10000);
            final BlockingQueue<ContentClaim> existing = destructableClaims.putIfAbsent(container, claimQueue);
            if (existing != null) {
                claimQueue = existing;
            }
        }

        return claimQueue;
    }

    @Override
    public void drainDestructableClaims(final String container, final Collection<ContentClaim> destination, final int maxElements) {
        final BlockingQueue<ContentClaim> destructableQueue = getDestructableClaimQueue(container);
        final int drainedCount = destructableQueue.drainTo(destination, maxElements);
        logger.debug("Drained {} destructable claims to {}", drainedCount, destination);
    }

    @Override
    public void drainDestructableClaims(final String container, final Collection<ContentClaim> destination, final int maxElements, final long timeout, final TimeUnit unit) {
        try {
            final BlockingQueue<ContentClaim> destructableQueue = getDestructableClaimQueue(container);
            final ContentClaim firstClaim = destructableQueue.poll(timeout, unit);
            if (firstClaim != null) {
                destination.add(firstClaim);
                destructableQueue.drainTo(destination, maxElements - 1);
            }
        } catch (final InterruptedException e) {
        }
    }

    @Override
    public void purge() {
        claimantCounts.clear();
    }

}
