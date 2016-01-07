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

package org.apache.giraph.comm.requests;

import org.apache.giraph.comm.messages.primitives.long_id.SenderIdIterationNumber;
import org.apache.giraph.utils.PairList;
import org.apache.giraph.utils.VertexIdData;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Abstract request to send a collection of data, indexed by vertex id,
 * for a partition.
 *
 * @param <I> Vertex id
 * @param <T> Data
 * @param <B> Specialization of
 * {@link org.apache.giraph.utils.VertexIdData} for T
 */
@SuppressWarnings("unchecked")
public abstract class SendWorkerDataRequest<I extends WritableComparable, T,
    B extends VertexIdData<I, T>>
    extends WritableRequest implements WorkerRequest {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(SendWorkerDataRequest.class);
  /**
   * All data for a group of vertices, organized by partition, which
   * are owned by a single (destination) worker. This data is all
   * destined for this worker.
   * */
  protected PairList<Integer, B> partitionVertexData;

  //Vicky workaround
  protected PairList<Integer, SenderIdIterationNumber> pId_sender_info;
  protected B vertex_data;

  /**
   * Constructor used for reflection only
   */
  public SendWorkerDataRequest() { }

  /**
   * Constructor used to send request.
   *
   * @param partVertData Map of remote partitions => VertexIdData
   */
  public SendWorkerDataRequest(
      PairList<Integer, B> partVertData) {
    this.partitionVertexData = partVertData;
  }

  /**
   * Create a new {@link org.apache.giraph.utils.VertexIdData}
   * specialized for the use case.
   *
   * @return A new instance of
   * {@link org.apache.giraph.utils.VertexIdData}
   */
  public abstract B createVertexIdData();

  @Override
  public void readFieldsRequest(DataInput input) throws IOException {
    int numPartitions = input.readInt();
    partitionVertexData = new PairList<Integer, B>();
    partitionVertexData.initialize(numPartitions);
    while (numPartitions-- > 0) {
      final int partitionId = input.readInt();
      B vertexIdData = createVertexIdData();
      vertexIdData.setConf(getConf());
      vertexIdData.readFields(input);
      partitionVertexData.add(partitionId, vertexIdData);
    }
  }

  @Override
  public void writeRequest(DataOutput output) throws IOException {
    output.writeInt(partitionVertexData.getSize());
    PairList<Integer, B>.Iterator
            iterator = partitionVertexData.getIterator();
    while (iterator.hasNext()) {
      iterator.next();
      output.writeInt(iterator.getCurrentFirst());
      iterator.getCurrentSecond().write(output);
    }
  }

  /*@Override
  public void readFieldsRequest(DataInput input) throws IOException {
    //LOG.debug("Vicky --> Read request");
    int size = input.readInt();
    pId_sender_info = new PairList<Integer, SenderIdIterationNumber>();
    pId_sender_info.initialize(size);
    while (size-- > 0) {
      final int partitionId = input.readInt(); //read pId
      //LOG.debug("Vicky --> readFieldsRequest: pId = " +  partitionId);
      I id = (I)new LongWritable(input.readLong()); //read sender Id
      //LOG.debug("Vicky --> readFieldsRequest: senderId = " +  id);
      long iter = input.readLong(); // read sender iteration
      //LOG.debug("Vicky --> readFieldsRequest: sender iteration = " +  iter);
      SenderIdIterationNumber<I> sender = new SenderIdIterationNumber<>(id,iter);
      int sz = input.readInt(); // read size of neighbors
      Set<I> neighbors = new HashSet<I>(sz);
      for(int i=0; i<sz; i++) {
        neighbors.add((I)new LongWritable(input.readLong())); // read neighbor id
      }
      sender.setNeighbors(neighbors);
      //LOG.debug("Vicky --> readFieldsRequest: neighbors = " +  neighbors);
      sender.initialize_map();

      //LOG.debug("Vicky --> readFieldsRequest: map = " +  sender.getMap());
      pId_sender_info.add(partitionId,sender);

      vertex_data = createVertexIdData();
      vertex_data.setConf(getConf());
      vertex_data.readFields(input);

      //LOG.debug("Vicky --> readFieldsRequest: message = " + vertex_data);
    }
  }

  @Override
  public void writeRequest(DataOutput output) throws IOException {
    LOG.debug("Vicky --> writeRequest");
    if(partitionVertexData != null) {
      output.writeInt(partitionVertexData.getSize());
      PairList<Integer, B>.Iterator
              iterator = partitionVertexData.getIterator();
      while (iterator.hasNext()) {
        iterator.next();
        output.writeInt(iterator.getCurrentFirst());
        iterator.getCurrentSecond().write(output);
      }
    }
    else {
      output.writeInt(pId_sender_info.getSize());
      PairList<Integer, SenderIdIterationNumber>.Iterator
              iterator = pId_sender_info.getIterator();
      while (iterator.hasNext()) {
        iterator.next();
        output.writeInt(iterator.getCurrentFirst()); // write pId
        //LOG.debug("Vicky --> writeRequest write pid = " + iterator.getCurrentFirst());
        ((SenderIdIterationNumber)iterator.getCurrentSecond()).getSenderId().write(output); // write sender Id
        //LOG.debug("Vicky --> writeRequest write sender id = " + ((SenderIdIterationNumber) iterator.getCurrentSecond()).getSenderId());
        output.writeLong(((SenderIdIterationNumber) iterator.getCurrentSecond()).getIteration()); // write sender iteration
        //LOG.debug("Vicky --> writeRequest write sender iteration = " + ((SenderIdIterationNumber) iterator.getCurrentSecond()).getIteration());
        int numValues = ((SenderIdIterationNumber)iterator.getCurrentSecond()).getNeighbors().size();
        output.writeInt(numValues);                 // write number of neighbors
        //LOG.debug("Vicky --> writeRequest number of neighbors = " + numValues);
        for (I n:(Set<I>) ((SenderIdIterationNumber)iterator.getCurrentSecond()).getNeighbors()) { // write neighbors
          n.write(output);
          //LOG.debug("Vicky --> writeRequest write neighbor = " + n);
        }

      }
      vertex_data.write(output); // write message
    }
  }*/



  @Override
  public int getSerializedSize() {
    int size = super.getSerializedSize() + 4;
    if(partitionVertexData != null) {
      PairList<Integer, B>.Iterator iterator = partitionVertexData.getIterator();
      while (iterator.hasNext()) {
        iterator.next();
        size += 4 + iterator.getCurrentSecond().getSerializedSize();
      }
    }
    else if(vertex_data != null) {
      size += 4 + vertex_data.getSerializedSize();
    }

    return size;
  }
}

