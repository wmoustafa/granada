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

import com.google.common.collect.Iterators;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.MessageClasses;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.*;
import org.apache.giraph.utils.io.DataInputOutput;
import org.apache.giraph.utils.io.ExtendedDataInputOutput;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

/**
 * Implementation of {@link SimpleMessageStore} where multiple messages are
 * stored per vertex as byte backed datastructures.
 * Used when there is no combiner provided.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public class ByteArrayMessagesPerVertexStore<I extends WritableComparable,
    M extends Writable> extends SimpleMessageStore<I, M, DataInputOutput> {

  /** Class logger */
  private static final Logger LOG =
        Logger.getLogger(ByteArrayMessagesPerVertexStore.class);


    /**
   * Constructor
   *
   * @param messageValueFactory Message class held in the store
   * @param service Service worker
   * @param config Hadoop configuration
   */
  public ByteArrayMessagesPerVertexStore(
      MessageValueFactory<M> messageValueFactory,
      CentralizedServiceWorker<I, ?, ?> service,
      ImmutableClassesGiraphConfiguration<I, ?, ?> config) {
    super(messageValueFactory, service, config);
  }

  @Override
  public boolean isPointerListEncoding() {
    return false;
  }


    @Override
    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "NP_LOAD_OF_KNOWN_NULL_VALUE",
            justification="I know what I am doing")
    public void addPartitionMessages( int partitionId, VertexIdMessages<I, M> messages) throws IOException {

        Object2ObjectOpenHashMap<I,DataInputOutput[]> partitionMap = new_map[partitionId];


        VertexIdMessageIterator<I, M> vertexIdMessageIterator = messages.getVertexIdMessageIterator();
        while (vertexIdMessageIterator.hasNext()) {
            vertexIdMessageIterator.next();
            I vertex_id = vertexIdMessageIterator.releaseCurrentVertexId();
            DataInputOutput[] vertex_map_entry = partitionMap.get(vertex_id);
            if (vertex_map_entry == null) {
                reentrant_locks[partitionId].writeLock().lock();
                try {
                    if (vertex_map_entry == null) {
                        vertex_map_entry = new DataInputOutput[2];
                        partitionMap.put(vertex_id, vertex_map_entry);
                        vertex_map_entry[0] = (config.createMessagesInputOutput());
                        vertex_map_entry[1] = (config.createMessagesInputOutput());
                        setCurrentIndex(partitionId, vertex_id, 0);
                    }
                }
                finally {
                    reentrant_locks[partitionId].writeLock().unlock();
                }
            }
            reentrant_locks[partitionId].readLock().lock();
            synchronized (vertex_map_entry) {
                try {
                    DataInputOutput dataInputOutput = vertex_map_entry[getCurrentIndex(partitionId, vertex_id)];
                    VerboseByteStructMessageWrite.verboseWriteCurrentMessage(vertexIdMessageIterator,
                            dataInputOutput.getDataOutput());
                    synchronized (has_messages_map[partitionId]) {
                        has_messages_map[partitionId].put(vertex_id, true);
                    }
                } finally {
                    reentrant_locks[partitionId].readLock().unlock();
                }
            }
        }

    }


    public Iterable<M> getVertexMessagesImproved(I vertexId, int pId) throws IOException {

        Object2ObjectOpenHashMap<I,DataInputOutput[]> partitionMap = new_map[pId];
        DataInputOutput[] vertex_map_entry = partitionMap.get(vertexId);
        if( vertex_map_entry == null)
        {
            return EmptyIterable.get();
        }
        reentrant_locks[pId].readLock().lock();
        try {
            int current_index = getCurrentIndex(pId,vertexId);
            DataInputOutput result =  vertex_map_entry[current_index];
            resetCurrentIndex(pId,vertexId, current_index);
            ((UnsafeByteArrayOutputStream) ((ExtendedDataInputOutput)  vertex_map_entry[getCurrentIndex(pId,vertexId)]).getDataOutput()).reset();
            synchronized (has_messages_map[pId]) {
                has_messages_map[pId].remove(vertexId);
            }
            return new MessagesIterable<M>(result, messageValueFactory);
        } finally {
        reentrant_locks[pId].readLock().unlock();
        }
    }


  @Override
  protected Iterable<M> getMessagesAsIterable(
      DataInputOutput dataInputOutput) {
    return new MessagesIterable<M>(dataInputOutput, messageValueFactory);
  }

  @Override
  protected int getNumberOfMessagesIn(
      ConcurrentMap<I, DataInputOutput> partitionMap) {
    int numberOfMessages = 0;
    for (DataInputOutput dataInputOutput : partitionMap.values()) {
      numberOfMessages += Iterators.size(
          new RepresentativeByteStructIterator<M>(
              dataInputOutput.createDataInput()) {
            @Override
            protected M createWritable() {
              return messageValueFactory.newInstance();
            }
          });
    }
    return numberOfMessages;
  }

  @Override
  protected void writeMessages(DataInputOutput dataInputOutput,
      DataOutput out) throws IOException {
    dataInputOutput.write(out);
  }

  @Override
  protected DataInputOutput readFieldsForMessages(DataInput in) throws
      IOException {
    DataInputOutput dataInputOutput = config.createMessagesInputOutput();
    dataInputOutput.readFields(in);
    return dataInputOutput;
  }

  /**
   * Create new factory for this message store
   *
   * @param service Worker service
   * @param config  Hadoop configuration
   * @param <I>     Vertex id
   * @param <M>     Message data
   * @return Factory
   */
  public static <I extends WritableComparable, M extends Writable>
  MessageStoreFactory<I, M, MessageStore<I, M>> newFactory(
      CentralizedServiceWorker<I, ?, ?> service,
      ImmutableClassesGiraphConfiguration<I, ?, ?> config) {
    return new Factory<I, M>(service, config);
  }

  /**
   * Factory for {@link ByteArrayMessagesPerVertexStore}
   *
   * @param <I> Vertex id
   * @param <M> Message data
   */
  private static class Factory<I extends WritableComparable, M extends Writable>
    implements MessageStoreFactory<I, M, MessageStore<I, M>> {
    /** Service worker */
    private CentralizedServiceWorker<I, ?, ?> service;
    /** Hadoop configuration */
    private ImmutableClassesGiraphConfiguration<I, ?, ?> config;

    /**
     * @param service Worker service
     * @param config  Hadoop configuration
     */
    public Factory(CentralizedServiceWorker<I, ?, ?> service,
        ImmutableClassesGiraphConfiguration<I, ?, ?> config) {
      this.service = service;
      this.config = config;
    }

    @Override
    public MessageStore<I, M> newStore(
        MessageClasses<I, M> messageClasses) {
      return new ByteArrayMessagesPerVertexStore<I, M>(
          messageClasses.createMessageValueFactory(config),
          service, config);
    }

    @Override
    public void initialize(CentralizedServiceWorker<I, ?, ?> service,
        ImmutableClassesGiraphConfiguration<I, ?, ?> conf) {
      this.service = service;
      this.config = conf;
    }

    @Override
    public boolean shouldTraverseMessagesInOrder() {
      return false;
    }
  }
}
