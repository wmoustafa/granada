/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.comm.messages;

import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import it.unimi.dsi.fastutil.objects.Object2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.UnsafeByteArrayOutputStream;
import org.apache.giraph.utils.io.DataInputOutput;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Abstract class for {@link MessageStore} which allows any kind
 * of object to hold messages for one vertex.
 * Simple in memory message store implemented with a two level concurrent
 * hash map.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 * @param <T> Type of object which holds messages for one vertex
 */
public abstract class SimpleMessageStore<I extends WritableComparable,
    M extends Writable, T> implements MessageStore<I, M>  {
  /** Class logger */
  private static final Logger LOG =
          Logger.getLogger(SimpleMessageStore.class);

  /** Message class */
  protected final MessageValueFactory<M> messageValueFactory;
  /** Service worker */
  protected final CentralizedServiceWorker<I, ?, ?> service;
  /** Map from partition id to map from vertex id to messages for that vertex */
  protected final ConcurrentMap<Integer, ConcurrentMap<I, T>> map;
  /** Giraph configuration */
  protected final ImmutableClassesGiraphConfiguration<I, ?, ?> config;

  /**
   * Vicky stuff
   */
  protected final Object2ObjectOpenHashMap[] new_map;
  protected Object2IntOpenHashMap[] current_position_map;
  protected Object2BooleanOpenHashMap[] has_messages_map;

  protected ReentrantReadWriteLock[] rw_locks;

  /**
   * Constructor
   *
   * @param messageValueFactory Message class held in the store
   * @param service Service worker
   * @param config Giraph configuration
   */
  public SimpleMessageStore(
      MessageValueFactory<M> messageValueFactory,
      CentralizedServiceWorker<I, ?, ?> service,
      ImmutableClassesGiraphConfiguration<I, ?, ?> config) {
    this.messageValueFactory = messageValueFactory;
    this.service = service;
    this.config = config;
    map = new MapMaker().concurrencyLevel(config.getNettyServerExecutionConcurrency()).makeMap();


    new_map = new Object2ObjectOpenHashMap[10000];
    for (int partitionId : service.getPartitionStore().getPartitionIds()) {
      Object2ObjectOpenHashMap<I,T[]> partitionMap = new Object2ObjectOpenHashMap<I,T[]>(
              (int) service.getPartitionStore().getPartitionVertexCount(partitionId));
      new_map[partitionId] = partitionMap;

    }

    current_position_map = new Object2IntOpenHashMap[10000];
    for (int partitionId : service.getPartitionStore().getPartitionIds()) {
      Object2IntOpenHashMap inner = new Object2IntOpenHashMap(
              (int) service.getPartitionStore().getPartitionVertexCount(partitionId));
      current_position_map[partitionId] =  inner;

    }

    has_messages_map = new Object2BooleanOpenHashMap[10000];
    for (int partitionId : service.getPartitionStore().getPartitionIds()) {
      Object2BooleanOpenHashMap inner = new Object2BooleanOpenHashMap(
              (int) service.getPartitionStore().getPartitionVertexCount(partitionId));
      has_messages_map[partitionId] =  inner;

    }

    rw_locks = new ReentrantReadWriteLock[10000];
    for (int partitionId : service.getPartitionStore().getPartitionIds()) {
      ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
      rw_locks[partitionId] = lock;
    }

  }

  /**
   * Get messages as an iterable from message storage
   *
   * @param messages Message storage
   * @return Messages as an iterable
   */
  protected abstract Iterable<M> getMessagesAsIterable(T messages);

  public Object2ObjectOpenHashMap<I,T> getPartitionMapByPartitionId( int pId) {
    return new_map[pId];
  }


  public Object2ObjectOpenHashMap<I,T>[] getMap() { return new_map; }


  public int getCurrentPos(int pId, I vId) {
    return current_position_map[pId].get(vId);
  }

  public void setCurrentPos(int pId, I vId, int pos) {
    current_position_map[pId].put(vId,pos);
  }

  public Object2IntOpenHashMap<I> get_position_map(int pId) { return this.current_position_map[pId]; }

  public Object2BooleanOpenHashMap<I> get_has_messages_map(int pId) { return this.has_messages_map[pId]; }


  public synchronized void resetAllVertexMessageMap(int pId) {

    if(new_map[pId] != null) {
      Object2ObjectOpenHashMap<I,T> vertex_map =  new_map[pId];
      for (I vertex_id : vertex_map.keySet()) {
        DataInputOutput messages = (DataInputOutput) vertex_map.get(vertex_id);
        Object2IntOpenHashMap pos_map = get_position_map(pId);
        if (!pos_map.containsKey(vertex_id)) {
          pos_map.put(vertex_id, 0);
          //continue;
        }
        int current_pos = getCurrentPos(pId, vertex_id);
        try {
          int elements = (((UnsafeByteArrayOutputStream) messages.getDataOutput()).getPos() - current_pos);
            /*LOG.info("Vicky --> vertex = " + vertex_id + ", " +
                    "current pos = " + current_pos +
                    ", end pos = " + ((UnsafeByteArrayOutputStream) messages.getDataOutput()).getPos() +
                    " , number of elements = " + elements);*/

          if (elements > 0) {
            //LOG.info("Vertex " + vertex_id + " has new messages");
            ((UnsafeByteArrayOutputStream) messages.getDataOutput()).vicky_write(
                    ((UnsafeByteArrayOutputStream) messages.getDataOutput()).getByteArray(),
                    current_pos, elements);
            has_messages_map[pId].put(vertex_id, true);
            //LOG.info("Vertex " + vertex_id + " has new messages");
          }
          else if (elements == 0) { // read all messages
            ((UnsafeByteArrayOutputStream) messages.getDataOutput()).reset();
          }
          pos_map.put(vertex_id, 0);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

  }

  public void clearHasNewMessages(int pId, I vertex_id) {
    //LOG.info("Clear has message flag for vertex = " + vertex_id);
    has_messages_map[pId].remove(vertex_id);
  }


  /**
   * Get number of messages in partition map
   *
   * @param partitionMap Partition map in which to count messages
   * @return Number of messages in partition map
   */
  protected abstract int getNumberOfMessagesIn(
      ConcurrentMap<I, T> partitionMap);

  /**
   * Write message storage to {@link DataOutput}
   *
   * @param messages Message storage
   * @param out Data output
   * @throws IOException
   */
  protected abstract void writeMessages(T messages, DataOutput out) throws
      IOException;

  /**
   * Read message storage from {@link DataInput}
   *
   * @param in Data input
   * @return Message storage
   * @throws IOException
   */
  protected abstract T readFieldsForMessages(DataInput in) throws IOException;

  /**
   * Get id of partition which holds vertex with selected id
   *
   * @param vertexId Id of vertex
   * @return Id of partiton
   */
  protected int getPartitionId(I vertexId) {
    return service.getVertexPartitionOwner(vertexId).getPartitionId();
  }

  /**
   * If there is already a map of messages related to the partition id
   * return that map, otherwise create a new one, put it in global map and
   * return it.
   *
   * @param partitionId Id of partition
   * @return Message map for this partition
   */
  protected ConcurrentMap<I, T> getOrCreatePartitionMap(int partitionId) {
    ConcurrentMap<I, T> partitionMap = map.get(partitionId);
    if (partitionMap == null) {
      ConcurrentMap<I, T> tmpMap = new MapMaker().concurrencyLevel(
          config.getNettyServerExecutionConcurrency()).makeMap();
      partitionMap = map.putIfAbsent(partitionId, tmpMap);
      if (partitionMap == null) {
        partitionMap = tmpMap;
      }
    }
    return partitionMap;
  }

  @Override
  public void finalizeStore() {
  }

  @Override
  public Iterable<I> getPartitionDestinationVertices(int partitionId) {
    ConcurrentMap<I, ?> partitionMap = map.get(partitionId);
    return (partitionMap == null) ? Collections.<I>emptyList() :
        partitionMap.keySet();
  }

  @Override
  public boolean hasMessagesForVertex(I vertexId) {
    ConcurrentMap<I, ?> partitionMap =
        map.get(getPartitionId(vertexId));
    return partitionMap != null && partitionMap.containsKey(vertexId);
  }

  @Override
  public Iterable<M> getVertexMessages(I vertexId) throws IOException {
    ConcurrentMap<I, T> partitionMap = map.get(getPartitionId(vertexId));
    if (partitionMap == null) {
      return Collections.<M>emptyList();
    }
    T messages = partitionMap.get(vertexId);
    return (messages == null) ? Collections.<M>emptyList() :
        getMessagesAsIterable(messages);
  }

  @Override
  public void writePartition(DataOutput out,
      int partitionId) throws IOException {
    ConcurrentMap<I, T> partitionMap = map.get(partitionId);
    out.writeBoolean(partitionMap != null);
    if (partitionMap != null) {
      out.writeInt(partitionMap.size());
      for (Map.Entry<I, T> entry : partitionMap.entrySet()) {
        entry.getKey().write(out);
        writeMessages(entry.getValue(), out);
      }
    }
  }

  @Override
  public void readFieldsForPartition(DataInput in,
      int partitionId) throws IOException {
    if (in.readBoolean()) {
      ConcurrentMap<I, T> partitionMap = Maps.newConcurrentMap();
      int numVertices = in.readInt();
      for (int v = 0; v < numVertices; v++) {
        I vertexId = config.createVertexId();
        vertexId.readFields(in);
        partitionMap.put(vertexId, readFieldsForMessages(in));
      }
      map.put(partitionId, partitionMap);
    }
  }

  @Override
  public void clearVertexMessages(I vertexId) throws IOException {
    ConcurrentMap<I, ?> partitionMap =
        map.get(getPartitionId(vertexId));
    if (partitionMap != null) {
      partitionMap.remove(vertexId);
    }
  }

  @Override
  public void clearPartition(int partitionId) throws IOException {
    map.remove(partitionId);
  }

  @Override
  public void clearAll() throws IOException {
    map.clear();
  }
}
