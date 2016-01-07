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

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.messages.MessagesIterable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.*;
import org.apache.giraph.utils.io.DataInputOutput;
import org.apache.giraph.utils.io.ExtendedDataInputOutput;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * Special message store to be used when ids are LongWritable and no combiner
 * is used.
 * Uses fastutil primitive maps in order to decrease number of objects and
 * get better performance.
 *
 * @param <M> Message type
 */
public class LongByteArrayMessageStore<M extends Writable>
  extends LongAbstractMessageStore<M, DataInputOutput> {

  // Vicky
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(LongByteArrayMessageStore.class);


  /**
   * Constructor
   *
   * @param messageValueFactory Factory for creating message values
   * @param service             Service worker
   * @param config              Hadoop configuration
   */
  public LongByteArrayMessageStore(
      MessageValueFactory<M> messageValueFactory,
      CentralizedServiceWorker<LongWritable, Writable, Writable> service,
      ImmutableClassesGiraphConfiguration<LongWritable,
          Writable, Writable> config) {
    super(messageValueFactory, service, config);
  }

  @Override
  public boolean isPointerListEncoding() {
    return false;
  }

  private DataInputOutput getDataInputOutput(int pId,
                                              Long2ObjectOpenHashMap<DataInputOutput[]> partitionMap,
                                              long vertexId) {
    DataInputOutput dataInputOutput = partitionMap.get(vertexId)[getCurrentIndex(pId, vertexId)];

    return dataInputOutput;
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "NP_LOAD_OF_KNOWN_NULL_VALUE",
          justification="I know what I am doing")
  public void addPartitionMessages(int partitionId,
                                   VertexIdMessages<LongWritable, M> messages) throws IOException {

    //Vicky
    //LOG.debug("Vicky --> : addPartitionMessages partitionId=[" + partitionId + "]");
    //setNewMessageFlag(partitionId, true);

    Long2ObjectOpenHashMap<DataInputOutput[]> partitionMap = new_map[partitionId];

      VertexIdMessageBytesIterator<LongWritable, M>
              vertexIdMessageBytesIterator =
              messages.getVertexIdMessageBytesIterator();
      // Try to copy the message buffer over rather than
      // doing a deserialization of a message just to know its size.  This
      // should be more efficient for complex objects where serialization is
      // expensive.  If this type of iterator is not available, fall back to
      // deserializing/serializing the messages
      if (vertexIdMessageBytesIterator != null) {
        //LOG.debug("Vicky --> addPartitionMessages vertexIdMessageBytesIterator");
        while (vertexIdMessageBytesIterator.hasNext()) {
          vertexIdMessageBytesIterator.next();
          DataInputOutput dataInputOutput = getDataInputOutput(partitionId,partitionMap,
                  vertexIdMessageBytesIterator.getCurrentVertexId().get());
          vertexIdMessageBytesIterator.writeCurrentMessageBytes(
                  dataInputOutput.getDataOutput());
        }
      } else {
        //LOG.debug("Vicky --> addPartitionMessages VerboseByteStructMessageWrite");
        VertexIdMessageIterator<LongWritable, M>
                iterator = messages.getVertexIdMessageIterator();
        while (iterator.hasNext()) {
          iterator.next();
          //LOG.info("Vicky --> addPartitionMessages Add message to dataInputOutput for " +
           //       "vId = " + iterator.getCurrentVertexId().get() + ", data =" + iterator.getCurrentData());

          //TODO add vertex to hasNewMessage
          //hasNewMessage(partitionId).add(iterator.getCurrentVertexId().get());
          long vertex_id = iterator.getCurrentVertexId().get();
          DataInputOutput[] vertex_map_entry = partitionMap.get(vertex_id);
          if (vertex_map_entry == null) {
            synchronized (partitionMap) {
              if (vertex_map_entry == null) {
                vertex_map_entry = new DataInputOutput[2];
                partitionMap.put(vertex_id, vertex_map_entry);
                vertex_map_entry[0] = (config.createMessagesInputOutput());
                vertex_map_entry[1] = (config.createMessagesInputOutput());
                setCurrentIndex(partitionId, vertex_id, 0);
              }
            }
          }
          synchronized (vertex_map_entry) {
            DataInputOutput dataInputOutput = vertex_map_entry[getCurrentIndex(partitionId, vertex_id)];
            VerboseByteStructMessageWrite.verboseWriteCurrentMessage(iterator,
                    dataInputOutput.getDataOutput());
            //has_messages_map[partitionId].put(vertex_id, true); //TODO for while
          }

     }
    }
  }

  @Override
  public Iterable<M> getVertexMessages(LongWritable vertexId) throws IOException {

    //LOG.info("Vicky --> getVertexMessages for vertexId=[" + vertexId + "]");

    int pId = service.getPartitionId(vertexId);
    Long2ObjectOpenHashMap<DataInputOutput[]> partitionMap = new_map[pId];
    DataInputOutput[] vertex_map_entry = partitionMap.get(vertexId.get());
    //TODO check if this is null or size 0
    if( vertex_map_entry == null)
    {
      return EmptyIterable.get();
    }
    synchronized ( vertex_map_entry) {
      int current_index = getCurrentIndex(pId,vertexId.get());
      DataInputOutput result =  vertex_map_entry[current_index];
      resetCurrentIndex(pId,vertexId.get(), current_index);
      ((UnsafeByteArrayOutputStream) ((ExtendedDataInputOutput)  vertex_map_entry[getCurrentIndex(pId,vertexId.get())]).getDataOutput()).reset();

      return new MessagesIterable<M>(result, messageValueFactory);
    }
  }

  public Iterable<M> getVertexMessagesImproved(LongWritable vertexId, int pId) throws IOException {

    //LOG.info("Vicky --> getVertexMessages for vertexId=[" + vertexId + "]");

    Long2ObjectOpenHashMap<DataInputOutput[]> partitionMap = new_map[pId];
    long vId = vertexId.get();
    DataInputOutput[] vertex_map_entry = partitionMap.get(vId);
    //TODO check if this is null or size 0
    if( vertex_map_entry == null)
    {
      return EmptyIterable.get();
    }
    synchronized ( vertex_map_entry) {
      int current_index = getCurrentIndex(pId,vId);
      DataInputOutput result =  vertex_map_entry[current_index];
      resetCurrentIndex(pId,vId, current_index);
      ((UnsafeByteArrayOutputStream) ((ExtendedDataInputOutput)  vertex_map_entry[getCurrentIndex(pId,vId)]).getDataOutput()).reset();
      //clearHasNewMessages(pId,vId); //TODO for while
      return new MessagesIterable<M>(result, messageValueFactory);
    }
  }



  @Override
  public void finalizeStore() {
  }




  public void printMap() {
    int size = 0;
    StringBuilder sb = new StringBuilder();
    for (int i : map.keySet()) {
      sb.append("pID: [" + i + "\n");
      synchronized (map.get(i).keySet()) {
          for (Long vId : map.get(i).keySet()) {
            sb.append("{vId: " + vId);
            DataInputOutput dataInputOutput = map.get(i).get(vId);
            if (dataInputOutput != null) {
              Iterable<M> messages = new MessagesIterable<M>(dataInputOutput, messageValueFactory);
              sb.append(" , messages =  ");
              for (M message : messages) {
                size++;
                sb.append(message + ", ");
              }
              sb.append("},");
            }
          }
          sb.append("]\n");
      }
      sb.append("\n");
    }
    //LOG.info("Vicky --> \n printExtendedMap of store size =  " + size +"\n" + sb.toString());
  }


  @Override
  public void writePartition(DataOutput out, int partitionId)
    throws IOException {
    Long2ObjectOpenHashMap<DataInputOutput> partitionMap =
        map.get(partitionId);
    out.writeInt(partitionMap.size());
    ObjectIterator<Long2ObjectMap.Entry<DataInputOutput>> iterator =
        partitionMap.long2ObjectEntrySet().fastIterator();
    while (iterator.hasNext()) {
      Long2ObjectMap.Entry<DataInputOutput> entry = iterator.next();
      out.writeLong(entry.getLongKey());
      entry.getValue().write(out);
    }
  }

  @Override
  public void readFieldsForPartition(DataInput in,
    int partitionId) throws IOException {
    int size = in.readInt();
    Long2ObjectOpenHashMap<DataInputOutput> partitionMap =
        new Long2ObjectOpenHashMap<DataInputOutput>(size);
    while (size-- > 0) {
      long vertexId = in.readLong();
      DataInputOutput dataInputOutput = config.createMessagesInputOutput();
      dataInputOutput.readFields(in);
      partitionMap.put(vertexId, dataInputOutput);
    }
    synchronized (map) {
      map.put(partitionId, partitionMap);
    }
  }
}
