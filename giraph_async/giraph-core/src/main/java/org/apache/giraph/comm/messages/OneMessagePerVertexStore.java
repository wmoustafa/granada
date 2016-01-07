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

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.MessageClasses;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.EmptyIterable;
import org.apache.giraph.utils.VertexIdMessageIterator;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ConcurrentMap;

/**
 * Implementation of {@link SimpleMessageStore} where we have a single
 * message per vertex.
 * Used when {@link org.apache.giraph.combiner.MessageCombiner} is provided.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public class OneMessagePerVertexStore<I extends WritableComparable,
    M extends Writable> extends SimpleMessageStore<I, M, M> {

  /** Class logger */
  private static final Logger LOG =
        Logger.getLogger(OneMessagePerVertexStore.class);


    /** MessageCombiner for messages */
  private final MessageCombiner<? super I, M> messageCombiner;

  /**
   * @param messageValueFactory Message class held in the store
   * @param service Service worker
   * @param messageCombiner MessageCombiner for messages
   * @param config Hadoop configuration
   */
  public OneMessagePerVertexStore(
      MessageValueFactory<M> messageValueFactory,
      CentralizedServiceWorker<I, ?, ?> service,
      MessageCombiner<? super I, M> messageCombiner,
      ImmutableClassesGiraphConfiguration<I, ?, ?> config) {
    super(messageValueFactory, service, config);
    this.messageCombiner =
        messageCombiner;
  }

  @Override
  public boolean isPointerListEncoding() {
    return false;
  }


    @Override
    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "NP_LOAD_OF_KNOWN_NULL_VALUE",
            justification="I know what I am doing")
    public void addPartitionMessages( int partitionId, VertexIdMessages<I, M> messages) throws IOException {

        Object2ObjectOpenHashMap<I,M[]> partitionMap = new_map[partitionId];


        VertexIdMessageIterator<I, M> vertexIdMessageIterator = messages.getVertexIdMessageIterator();
        while (vertexIdMessageIterator.hasNext()) {
            vertexIdMessageIterator.next();

            I vertex_id = vertexIdMessageIterator.releaseCurrentVertexId();
            M[] vertex_map_entry = partitionMap.get(vertex_id);
            if (vertex_map_entry == null) {
                reentrant_locks[partitionId].writeLock().lock();
                try {
                    if (vertex_map_entry == null) {
                        vertex_map_entry = (M[]) new Writable[2];
                        partitionMap.put(vertex_id, vertex_map_entry);
                        vertex_map_entry[0] = messageCombiner.createInitialMessage();
                        vertex_map_entry[1] = messageCombiner.createInitialMessage();
                        setCurrentIndex(partitionId, vertex_id, 0);

                    }
                } finally {
                    reentrant_locks[partitionId].writeLock().unlock();
                }
            }
            reentrant_locks[partitionId].readLock().lock();
            synchronized (vertex_map_entry) {
                try {
                    messageCombiner.combine(vertex_id, vertex_map_entry[getCurrentIndex(partitionId, vertex_id)],
                            vertexIdMessageIterator.getCurrentMessage());
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

        Object2ObjectOpenHashMap<I,M[]> partitionMap = new_map[pId];
        M[] vertex_map_entry = partitionMap.get(vertexId);
        if( vertex_map_entry == null)
        {
            return EmptyIterable.get();
        }

        reentrant_locks[pId].readLock().lock();
        try {
            int current_index = getCurrentIndex(pId, vertexId);
            M result = vertex_map_entry[current_index];
            resetCurrentIndex(pId, vertexId, current_index);
            vertex_map_entry[getCurrentIndex(pId, vertexId)] = messageCombiner.createInitialMessage();
            synchronized (has_messages_map[pId]) {
                has_messages_map[pId].remove(vertexId);
            }
            return Collections.singleton(result);
        } finally {
            reentrant_locks[pId].readLock().unlock();
        }

    }


    @Override
  protected Iterable<M> getMessagesAsIterable(M message) {
    return Collections.singleton(message);
  }

  @Override
  protected int getNumberOfMessagesIn(ConcurrentMap<I, M> partitionMap) {
    return partitionMap.size();
  }

  @Override
  protected void writeMessages(M messages, DataOutput out) throws IOException {
    messages.write(out);
  }

  @Override
  protected M readFieldsForMessages(DataInput in) throws IOException {
    M message = messageValueFactory.newInstance();
    message.readFields(in);
    return message;
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
   * Factory for {@link OneMessagePerVertexStore}
   *
   * @param <I> Vertex id
   * @param <M> Message data
   */
  private static class Factory<I extends WritableComparable,
      M extends Writable>
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
      return new OneMessagePerVertexStore<I, M>(
          messageClasses.createMessageValueFactory(config), service,
          messageClasses.createMessageCombiner(config), config);
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
