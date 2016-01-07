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

/**
 * Created by Vicky Papavasileiou on 7/31/15.
 */

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Special message store to be used when ids are LongWritable and no combiner
 * is used.
 * Uses fastutil primitive maps in order to decrease number of objects and
 * get better performance.
 *
 * @param <M> message type
 * @param <T> datastructure used to hold messages
 */
public abstract class LongAbstractExtendedMessageStore<M extends Writable, T>
        implements MessageStore<LongWritable, M> {

    // Vicky
    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(LongAbstractExtendedMessageStore.class);

    /** Message value factory */
    protected final MessageValueFactory<M> messageValueFactory;
    /** Map from partition id to map from sender id to map from vertex id to message */
    protected final
    Int2ObjectOpenHashMap<Map<SenderIdIterationNumber,Long2ObjectOpenHashMap<T>>> map;
            //Map<Integer, Map<SenderIdIterationNumber,Map<Long,T>>> map;

    /** Service worker */
    protected final CentralizedServiceWorker<LongWritable, ?, ?> service;
    /** Giraph configuration */
    protected final ImmutableClassesGiraphConfiguration<LongWritable, ?, ?>
            config;

    /**
     * Constructor
     *
     * @param messageValueFactory Factory for creating message values
     * @param service      Service worker
     * @param config       Hadoop configuration
     */
    public LongAbstractExtendedMessageStore(
            MessageValueFactory<M> messageValueFactory,
            CentralizedServiceWorker<LongWritable, Writable, Writable> service,
            ImmutableClassesGiraphConfiguration<LongWritable, Writable, Writable>
                    config) {
        this.messageValueFactory = messageValueFactory;
        this.service = service;
        this.config = config;

        map = new Int2ObjectOpenHashMap<Map<SenderIdIterationNumber,Long2ObjectOpenHashMap<T>>>();
        for (int partitionId : service.getPartitionStore().getPartitionIds()) {
            Map<SenderIdIterationNumber,Long2ObjectOpenHashMap<T>> partitionMap =
                    new ConcurrentHashMap<SenderIdIterationNumber,Long2ObjectOpenHashMap<T>>(
                    (int) service.getPartitionStore()
                            .getPartitionVertexCount(partitionId));
            map.put(partitionId, partitionMap);

        }
    }

    /**
     * Get map which holds messages for partition which vertex belongs to.
     *
     * @param vertexId Id of the vertex
     * @return Map which holds messages for partition which vertex belongs to.
     */
    protected Map<SenderIdIterationNumber,Long2ObjectOpenHashMap<T>> getPartitionMap(
            LongWritable vertexId) {
        LOG.debug("Vicky --> getPartitionMap message store = " + this);
        LOG.debug("Vicky --> getPartitionMap partitionId=" + service.getPartitionId(vertexId) + " for vertexid = " + vertexId);
        return map.get(service.getPartitionId(vertexId));
    }

    /**
     * Vicky
     * Get map which holds messages for partition which vertex belongs to.
     *
     * @param pId Id of the vertex
     * @return Map which holds messages for partition which vertex belongs to.
     */
    public Map<SenderIdIterationNumber,Long2ObjectOpenHashMap<T>>
    getExtendedPartitionMap( int pId) {

        return map.get(pId);
    }


    @Override
    public void clearPartition(int partitionId) throws IOException {
        map.get(partitionId).clear();
    }

    @Override
    public boolean hasMessagesForVertex(LongWritable vertexId) {
        return getPartitionMap(vertexId).containsKey(vertexId.get());
    }

    @Override
    public void clearVertexMessages(LongWritable vertexId) throws IOException {
        LOG.debug("Vicky --> clearVertexMessages");
        //LOG.debug("Vicky --> partitionMap contains for vertex " + vertexId +
        //        " messages =  " + getPartitionMap(vertexId).toString());
        getPartitionMap(vertexId).remove(vertexId.get());
    }

    @Override
    public void clearAll() throws IOException {
        map.clear();
    }

    @Override
    public Iterable<LongWritable> getPartitionDestinationVertices(
            int partitionId) {
        throw new UnsupportedOperationException();
    }

}
