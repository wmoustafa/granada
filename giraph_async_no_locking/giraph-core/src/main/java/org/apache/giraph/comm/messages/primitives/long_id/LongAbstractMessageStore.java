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

package org.apache.giraph.comm.messages.primitives.long_id;

import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.longs.Long2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.UnsafeByteArrayOutputStream;
import org.apache.giraph.utils.io.DataInputOutput;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;


/**
 * Special message store to be used when ids are LongWritable and no combiner
 * is used.
 * Uses fastutil primitive maps in order to decrease number of objects and
 * get better performance.
 *
 * @param <M> message type
 * @param <T> datastructure used to hold messages
 */
public abstract class LongAbstractMessageStore<M extends Writable, T>
  implements MessageStore<LongWritable, M> {

  // Vicky
  /** Class logger */
  private static final Logger LOG =
          Logger.getLogger(LongAbstractMessageStore.class);

  /** Message value factory */
  protected final MessageValueFactory<M> messageValueFactory;
  /** Map from partition id to map from vertex id to message */
  protected final Long2ObjectOpenHashMap<T>[] map;

  /** Service worker */
  protected final CentralizedServiceWorker<LongWritable, ?, ?> service;
  /** Giraph configuration */
  protected final ImmutableClassesGiraphConfiguration<LongWritable, ?, ?> config;


  /**
   * Vicky stuff
   */
  protected Long2IntOpenHashMap[] current_position_map;
  protected Long2BooleanOpenHashMap[] has_messages_map;

  /**
   * Constructor
   *
   * @param messageValueFactory Factory for creating message values
   * @param service      Service worker
   * @param config       Hadoop configuration
   */
  public LongAbstractMessageStore(
      MessageValueFactory<M> messageValueFactory,
      CentralizedServiceWorker<LongWritable, Writable, Writable> service,
      ImmutableClassesGiraphConfiguration<LongWritable, Writable, Writable>
          config) {
    this.messageValueFactory = messageValueFactory;
    this.service = service;
    this.config = config;


    map = new Long2ObjectOpenHashMap[300];
    for (int partitionId : service.getPartitionStore().getPartitionIds()) {
      Long2ObjectOpenHashMap<T> partitionMap = new Long2ObjectOpenHashMap<T>(
          (int) service.getPartitionStore().getPartitionVertexCount(partitionId));
      map[partitionId] =  partitionMap;

    }


    current_position_map = new Long2IntOpenHashMap[300];
    for (int partitionId : service.getPartitionStore().getPartitionIds()) {
      Long2IntOpenHashMap inner = new Long2IntOpenHashMap(
              (int) service.getPartitionStore().getPartitionVertexCount(partitionId));
      current_position_map[partitionId] =  inner;

    }

    has_messages_map = new Long2BooleanOpenHashMap[300];
    for (int partitionId : service.getPartitionStore().getPartitionIds()) {
      Long2BooleanOpenHashMap inner = new Long2BooleanOpenHashMap(
              (int) service.getPartitionStore().getPartitionVertexCount(partitionId));
      has_messages_map[partitionId] =  inner;

    }


  }


  /**
   * Get map which holds messages for partition which vertex belongs to.
   *
   * @param vertexId Id of the vertex
   * @return Map which holds messages for partition which vertex belongs to.
   */
  protected Long2ObjectOpenHashMap<T> getPartitionMap( LongWritable vertexId) {
    return map[service.getPartitionId(vertexId)];
  }

  public Long2ObjectOpenHashMap<T> getPartitionMapByPartitionId( int pId) {
    return map[pId];
  }


  public Long2ObjectOpenHashMap<T>[] getMap() { return map; }


  public int getCurrentPos(int pId, long vId) {
    return current_position_map[pId].get(vId);
  }

  public void setCurrentPos(int pId, long vId, int pos) {
    current_position_map[pId].put(vId,pos);
  }

  public Long2IntOpenHashMap get_position_map(int pId) { return this.current_position_map[pId]; }

  public Long2BooleanOpenHashMap get_has_messages_map(int pId) { return this.has_messages_map[pId]; }

  @Override
  public void clearPartition(int partitionId) throws IOException {
    map[partitionId].clear();
  }

  @Override
  public boolean hasMessagesForVertex(LongWritable vertexId) {
    return getPartitionMap(vertexId).containsKey(vertexId.get());
  }


  public void resetAllVertexMessageMap(int pId) {

      if(map[pId] != null) {
        Long2ObjectOpenHashMap vertex_map = ((Long2ObjectOpenHashMap) map[pId]);
        for (long vertex_id : vertex_map.keySet()) {
          DataInputOutput messages = (DataInputOutput) vertex_map.get(vertex_id);
          Long2IntOpenHashMap pos_map = get_position_map(pId);
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
            LOG.error(e.getMessage());
            e.printStackTrace();
          }
        }
      }

  }

  public void clearHasNewMessages(int pId, long vertex_id) {
    //LOG.info("Clear has message flag for vertex = " + vertex_id);
    has_messages_map[pId].remove(vertex_id);
  }

  public void resetVertexMessageMap(int pId, long vertex_id) {

    if(map[pId] != null) {
      Long2ObjectOpenHashMap vertex_map = ((Long2ObjectOpenHashMap) map[pId]);
      DataInputOutput messages = (DataInputOutput) vertex_map.get(vertex_id);
      Long2IntOpenHashMap pos_map = get_position_map(pId);
      if (!pos_map.containsKey(vertex_id)) {
        return;
      }
      int current_pos = getCurrentPos(pId, vertex_id);
      try {
        int elements = (((UnsafeByteArrayOutputStream) messages.getDataOutput()).getPos() - current_pos);

        if (elements > 0) {
          ((UnsafeByteArrayOutputStream) messages.getDataOutput()).vicky_write(
                  ((UnsafeByteArrayOutputStream) messages.getDataOutput()).getByteArray(),
                  current_pos, elements);
          has_messages_map[pId].put(vertex_id,true);
        }
        else if (elements == 0) { // read all messages
          ((UnsafeByteArrayOutputStream) messages.getDataOutput()).reset();
        }
        pos_map.put(vertex_id, 0);
      } catch (IOException e) {
        LOG.error(e.getMessage());
        e.printStackTrace();
      }
    }
  }

  @Override
  public void clearVertexMessages(LongWritable vertexId) throws IOException {
    //LOG.debug("Vicky --> partitionMap contains for vertex " + vertexId +
    //        " messages =  " + getPartitionMap(vertexId).toString());
    getPartitionMap(vertexId).remove(vertexId.get());
  }

  @Override
  public void clearAll() throws IOException {
    for(int i=0; i<map.length; i++) {
      map[i] = null;
    }
  }

  @Override
  public Iterable<LongWritable> getPartitionDestinationVertices( int partitionId) {
    Long2ObjectOpenHashMap<T> partitionMap = map[partitionId];
    List<LongWritable> vertices =
        Lists.newArrayListWithCapacity(partitionMap.size());
    LongIterator iterator = partitionMap.keySet().iterator();
    while (iterator.hasNext()) {
      vertices.add(new LongWritable(iterator.nextLong()));
    }
    return vertices;
  }

}
