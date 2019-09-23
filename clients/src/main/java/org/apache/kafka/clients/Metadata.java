/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A class encapsulating some of the logic around metadata.
 * <p>
 * This class is shared by the client thread (for partitioning) and the background sender thread.
 * <p>
 * Metadata is maintained for only a subset of topics, which can be added to over time. When we request metadata for a
 * topic we don't have any metadata for it will trigger a metadata update.
 * <p>
 * If topic expiry is enabled for the metadata, any topic that has not been used within the expiry interval
 * is removed from the metadata refresh set after an update. Consumers disable topic expiry since they explicitly
 * manage topics while producers rely on topic expiry to limit the refresh set.
 */

// 这个类被 client 线程和后台 sender 所共享,它只保存了所有 topic 的部分数据,当我们请求一个它上面没有的 topic meta 时,
// 它会通过发送 metadata update 来更新 meta 信息,
// 如果 topic meta 过期策略是允许的,那么任何 topic 过期的话都会被从集合中移除,
// 但是 consumer 是不允许 topic 过期的因为它明确地知道它需要管理哪些 topic
public final class Metadata {

   private static final Logger log = LoggerFactory.getLogger(Metadata.class);

   public static final long TOPIC_EXPIRY_MS = 5 * 60 * 1000;
   private static final long TOPIC_EXPIRY_NEEDS_UPDATE = -1L;

   private final long refreshBackoffMs; // metadata 更新失败时,为避免频繁更新 meta,最小的间隔时间,默认 100ms
   private final long metadataExpireMs; // metadata 的过期时间, 默认 60,000ms
   private int version; // 每更新成功1次，version自增1,主要是用于判断 metadata 是否更新
   private long lastRefreshMs; // 最近一次更新时的时间（包含更新失败的情况）
   private long lastSuccessfulRefreshMs; // 最近一次成功更新的时间（如果每次都成功的话，与前面的值相等, 否则，lastSuccessulRefreshMs < lastRefreshMs)
   private Cluster cluster; // 集群中一些 topic 的信息
   private boolean needUpdate; // 是都需要更新 metadata
   /* Topics with expiry time */
   private final Map<String, Long> topics; // topic 与其过期时间的对应关系
   private final List<Listener> listeners; // 事件监控者
   private final ClusterResourceListeners clusterResourceListeners; //当接收到 metadata 更新时, ClusterResourceListeners的列表
   private boolean needMetadataForAllTopics; // 是否强制更新所有的 metadata
   private final boolean topicExpiryEnabled; // 默认为 true, Producer 会定时移除过期的 topic,consumer 则不会移除
   private final boolean allowAutoTopicCreation;
   private AuthenticationException authenticationException;

   public Metadata(long refreshBackoffMs, long metadataExpireMs, boolean allowAutoTopicCreation) {
      this(refreshBackoffMs, metadataExpireMs, allowAutoTopicCreation, false, new ClusterResourceListeners());
   }

   /**
    * Create a new Metadata instance
    *
    * @param refreshBackoffMs         The minimum amount of time that must expire between metadata refreshes to avoid busy
    *                                 polling
    * @param metadataExpireMs         The maximum amount of time that metadata can be retained without refresh
    * @param allowAutoTopicCreation   If this and the broker config 'auto.create.topics.enable' are true, topics that
    *                                 don't exist will be created by the broker when a metadata request is sent
    * @param topicExpiryEnabled       If true, enable expiry of unused topics
    * @param clusterResourceListeners List of ClusterResourceListeners which will receive metadata updates.
    */
   public Metadata(long refreshBackoffMs, long metadataExpireMs, boolean allowAutoTopicCreation,
                   boolean topicExpiryEnabled, ClusterResourceListeners clusterResourceListeners) {
      this.refreshBackoffMs = refreshBackoffMs;
      this.metadataExpireMs = metadataExpireMs;
      this.allowAutoTopicCreation = allowAutoTopicCreation;
      this.topicExpiryEnabled = topicExpiryEnabled;
      this.lastRefreshMs = 0L;
      this.lastSuccessfulRefreshMs = 0L;
      this.version = 0;
      this.cluster = Cluster.empty();
      this.needUpdate = false;
      this.topics = new HashMap<>();
      this.listeners = new ArrayList<>();
      this.clusterResourceListeners = clusterResourceListeners;
      this.needMetadataForAllTopics = false;
   }

   /**
    * Get the current cluster info without blocking
    */
   public synchronized Cluster fetch() {
      return this.cluster;
   }

   /**
    * Add the topic to maintain in the metadata. If topic expiry is enabled, expiry time
    * will be reset on the next update.
    */
   public synchronized void add(String topic) {
      Objects.requireNonNull(topic, "topic cannot be null");
      if (topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE) == null) {
         requestUpdateForNewTopics();
      }
   }

   /**
    * The next time to update the cluster info is the maximum of the time the current info will expire and the time the
    * current info can be updated (i.e. backoff time has elapsed); If an update has been request then the expiry time
    * is now
    */
   public synchronized long timeToNextUpdate(long nowMs) {
      long timeToExpire = needUpdate ? 0 : Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0);
      long timeToAllowUpdate = this.lastRefreshMs + this.refreshBackoffMs - nowMs;
      return Math.max(timeToExpire, timeToAllowUpdate);
   }

   /**
    * Request an update of the current cluster metadata info, return the current version before the update
    */
   public synchronized int requestUpdate() {
      this.needUpdate = true;
      return this.version;
   }

   /**
    * Check whether an update has been explicitly requested.
    *
    * @return true if an update was requested, false otherwise
    */
   public synchronized boolean updateRequested() {
      return this.needUpdate;
   }

   /**
    * If any non-retriable authentication exceptions were encountered during
    * metadata update, clear and return the exception.
    */
   public synchronized AuthenticationException getAndClearAuthenticationException() {
      if (authenticationException != null) {
         AuthenticationException exception = authenticationException;
         authenticationException = null;
         return exception;
      } else {
         return null;
      }
   }

   /**
    * Wait for metadata update until the current version is larger than the last version we know of
    */
   // 更新 metadata 信息（根据当前 version 值来判断）
   public synchronized void awaitUpdate(final int lastVersion, final long maxWaitMs) throws InterruptedException {
      if (maxWaitMs < 0) {
         throw new IllegalArgumentException("Max time to wait for metadata updates should not be < 0 milliseconds");
      }
      long begin = System.currentTimeMillis();
      long remainingWaitMs = maxWaitMs;
      while (this.version <= lastVersion) {// 不断循环,直到 metadata 更新成功,version 自增
         AuthenticationException ex = getAndClearAuthenticationException();
         if (ex != null) {
            throw ex;
         }
         if (remainingWaitMs != 0) {
            wait(remainingWaitMs);// 阻塞线程，等待 metadata 的更新
         }
         long elapsed = System.currentTimeMillis() - begin;
         if (elapsed >= maxWaitMs) {// timeout
            throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
         }
         remainingWaitMs = maxWaitMs - elapsed;
      }
   }

   /**
    * Replace the current set of topics maintained to the one provided.
    * If topic expiry is enabled, expiry time of the topics will be
    * reset on the next update.
    *
    * @param topics
    */
   public synchronized void setTopics(Collection<String> topics) {
      if (!this.topics.keySet().containsAll(topics)) {
         requestUpdateForNewTopics();
      }
      this.topics.clear();
      for (String topic : topics)
         this.topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE);
   }

   /**
    * Get the list of topics we are currently maintaining metadata for
    */
   public synchronized Set<String> topics() {
      return new HashSet<>(this.topics.keySet());
   }

   /**
    * Check if a topic is already in the topic set.
    *
    * @param topic topic to check
    * @return true if the topic exists, false otherwise
    */
   public synchronized boolean containsTopic(String topic) {
      return this.topics.containsKey(topic);
   }

   /**
    * Updates the cluster metadata. If topic expiry is enabled, expiry time
    * is set for topics if required and expired topics are removed from the metadata.
    *
    * @param cluster           the cluster containing metadata for topics with valid metadata
    * @param unavailableTopics topics which are non-existent or have one or more partitions whose
    *                          leader is not known
    * @param now               current time in milliseconds
    */
   public synchronized void update(Cluster cluster, Set<String> unavailableTopics, long now) {
      Objects.requireNonNull(cluster, "cluster should not be null");

      this.needUpdate = false;
      this.lastRefreshMs = now;
      this.lastSuccessfulRefreshMs = now;
      this.version += 1;

      if (topicExpiryEnabled) {
         // Handle expiry of topics from the metadata refresh set.
         for (Iterator<Map.Entry<String, Long>> it = topics.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Long> entry = it.next();
            long expireMs = entry.getValue();
            if (expireMs == TOPIC_EXPIRY_NEEDS_UPDATE) {
               entry.setValue(now + TOPIC_EXPIRY_MS);
            } else if (expireMs <= now) {
               it.remove();
               log.debug("Removing unused topic {} from the metadata list, expiryMs {} now {}", entry.getKey(), expireMs, now);
            }
         }
      }

      for (Listener listener : listeners) {
         listener.onMetadataUpdate(cluster, unavailableTopics);
      }

      String previousClusterId = cluster.clusterResource().clusterId();

      if (this.needMetadataForAllTopics) {
         // the listener may change the interested topics, which could cause another metadata refresh.
         // If we have already fetched all topics, however, another fetch should be unnecessary.
         this.needUpdate = false;
         this.cluster = getClusterForCurrentTopics(cluster);
      } else {
         this.cluster = cluster;
      }

      // The bootstrap cluster is guaranteed not to have any useful information
      if (!cluster.isBootstrapConfigured()) {
         String clusterId = cluster.clusterResource().clusterId();
         if (clusterId == null ? previousClusterId != null : !clusterId.equals(previousClusterId)) {
            log.info("Cluster ID: {}", cluster.clusterResource().clusterId());
         }
         clusterResourceListeners.onUpdate(cluster.clusterResource());
      }

      notifyAll();
      log.debug("Updated cluster metadata version {} to {}", this.version, this.cluster);
   }

   /**
    * Record an attempt to update the metadata that failed. We need to keep track of this
    * to avoid retrying immediately.
    */
   public synchronized void failedUpdate(long now, AuthenticationException authenticationException) {
      this.lastRefreshMs = now;
      this.authenticationException = authenticationException;
      if (authenticationException != null)
         this.notifyAll();
   }

   /**
    * @return The current metadata version
    */
   public synchronized int version() {
      return this.version;
   }

   /**
    * The last time metadata was successfully updated.
    */
   public synchronized long lastSuccessfulUpdate() {
      return this.lastSuccessfulRefreshMs;
   }

   public boolean allowAutoTopicCreation() {
      return allowAutoTopicCreation;
   }

   /**
    * Set state to indicate if metadata for all topics in Kafka cluster is required or not.
    *
    * @param needMetadataForAllTopics boolean indicating need for metadata of all topics in cluster.
    */
   public synchronized void needMetadataForAllTopics(boolean needMetadataForAllTopics) {
      if (needMetadataForAllTopics && !this.needMetadataForAllTopics) {
         requestUpdateForNewTopics();
      }
      this.needMetadataForAllTopics = needMetadataForAllTopics;
   }

   /**
    * Get whether metadata for all topics is needed or not
    */
   public synchronized boolean needMetadataForAllTopics() {
      return this.needMetadataForAllTopics;
   }

   /**
    * Add a Metadata listener that gets notified of metadata updates
    */
   public synchronized void addListener(Listener listener) {
      this.listeners.add(listener);
   }

   /**
    * Stop notifying the listener of metadata updates
    */
   public synchronized void removeListener(Listener listener) {
      this.listeners.remove(listener);
   }

   /**
    * MetadataUpdate Listener
    */
   public interface Listener {
      /**
       * Callback invoked on metadata update.
       *
       * @param cluster           the cluster containing metadata for topics with valid metadata
       * @param unavailableTopics topics which are non-existent or have one or more partitions whose
       *                          leader is not known
       */
      void onMetadataUpdate(Cluster cluster, Set<String> unavailableTopics);
   }

   private synchronized void requestUpdateForNewTopics() {
      // Override the timestamp of last refresh to let immediate update.
      this.lastRefreshMs = 0;
      requestUpdate();
   }

   private Cluster getClusterForCurrentTopics(Cluster cluster) {
      Set<String> unauthorizedTopics = new HashSet<>();
      Collection<PartitionInfo> partitionInfos = new ArrayList<>();
      List<Node> nodes = Collections.emptyList();
      Set<String> internalTopics = Collections.emptySet();
      Node controller = null;
      String clusterId = null;
      if (cluster != null) {
         clusterId = cluster.clusterResource().clusterId();
         internalTopics = cluster.internalTopics();
         unauthorizedTopics.addAll(cluster.unauthorizedTopics());
         unauthorizedTopics.retainAll(this.topics.keySet());

         for (String topic : this.topics.keySet()) {
            List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
            if (!partitionInfoList.isEmpty()) {
               partitionInfos.addAll(partitionInfoList);
            }
         }
         nodes = cluster.nodes();
         controller = cluster.controller();
      }
      return new Cluster(clusterId, nodes, partitionInfos, unauthorizedTopics, internalTopics, controller);
   }
}
