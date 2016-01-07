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

import org.apache.giraph.comm.ServerData;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.PairList;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Send a collection of vertex messages for a partition.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
@SuppressWarnings("unchecked")
public class SendWorkerMessagesRequest<I extends WritableComparable,
    M extends Writable> extends SendWorkerDataRequest<I, M,
    VertexIdMessages<I, M>> {

    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(SendWorkerMessagesRequest.class);


    /*Vicky*/
    private long addMessageTime = 0;
    private long addMessageCounter = 0;


    /** Default constructor */
  public SendWorkerMessagesRequest() {
  }

  /**
   * Constructor used to send request.
   *
   * @param partVertMsgs Map of remote partitions =>
   *                     VertexIdMessages
   */
  public SendWorkerMessagesRequest(
      PairList<Integer, VertexIdMessages<I, M>> partVertMsgs) {
    this.partitionVertexData = partVertMsgs;
      addMessageTime = 0;
      addMessageCounter = 0;
  }

  @Override
  public VertexIdMessages<I, M> createVertexIdData() {
    return new ByteArrayVertexIdMessages<I, M>(
        getConf().createOutgoingMessageValueFactory());
  }

    public long getAddMessageTime(){ return this.addMessageTime; }
    public long getAddMessageCounter(){ return this.addMessageCounter; }

    public void setAddMessageTime(long addMessageTime) {
        this.addMessageTime = addMessageTime;
    }


  @Override
  public RequestType getType() {
    return RequestType.SEND_WORKER_MESSAGES_REQUEST;
  }

  @Override
  public void doRequest(ServerData serverData) {
      long start = System.currentTimeMillis();
    PairList<Integer, VertexIdMessages<I, M>>.Iterator
        iterator = partitionVertexData.getIterator();
    while (iterator.hasNext()) {
      iterator.next();
      try {
        serverData.getIncomingMessageStore().
            addPartitionMessages(iterator.getCurrentFirst(),
                iterator.getCurrentSecond());
          addMessageCounter++;
      } catch (IOException e) {
        throw new RuntimeException("doRequest: Got IOException ", e);
      }
    }
      long end = System.currentTimeMillis();
      addMessageTime +=(end-start);
  }
}
