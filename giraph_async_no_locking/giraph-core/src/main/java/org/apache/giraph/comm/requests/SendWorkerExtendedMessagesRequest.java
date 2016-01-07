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

/**
 * Created by Vicky Papavasileiou on 7/31/15.
 */

import org.apache.giraph.comm.ServerData;
import org.apache.giraph.comm.messages.primitives.long_id.LongByteArrayExtendedMessageStore;
import org.apache.giraph.utils.ByteArraySenderIdVertexIdMessages;
import org.apache.giraph.utils.PairList;
import org.apache.giraph.utils.SenderIdVertexIdMessages;
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
public class SendWorkerExtendedMessagesRequest<I extends WritableComparable,
        M extends Writable> extends SendWorkerExtendedDataRequest<I, M,
        SenderIdVertexIdMessages<I, M>> {

    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(SendWorkerMessagesRequest.class);

    /** Default constructor */
    public SendWorkerExtendedMessagesRequest() {
    }


    /**
     * Vicky
     * Constructor used to send extended request.
     *
     * @param v_data Map of remote partitions =>
     *                     VertexIdMessages
     */
    public SendWorkerExtendedMessagesRequest(
            PairList<Integer, SenderIdVertexIdMessages<I, M>> v_data) {
        this.partitionVertexData = v_data;
    }

    @Override
    public SenderIdVertexIdMessages<I, M> createSenderIdVertexIdData() {
        return new ByteArraySenderIdVertexIdMessages<I, M>(
                getConf().createOutgoingMessageValueFactory());
    }

    @Override
    public RequestType getType() {
        return RequestType.SEND_WORKER_EXTENDED_MESSAGES_REQUEST;
    }

  @Override
  public void doRequest(ServerData serverData) {
    PairList<Integer, SenderIdVertexIdMessages<I, M>>.Iterator
        iterator = partitionVertexData.getIterator();
    LOG.debug("Vicky --> In doRequst:");
    while (iterator.hasNext()) {
      iterator.next();
      try {
        LOG.debug("Vicky --> doRequest: add messages to incoming message store");
          ((LongByteArrayExtendedMessageStore)serverData.getIncomingExtendedMessageStore()).
            addExtendedPartitionMessages(iterator.getCurrentFirst(),
                iterator.getCurrentSecond());
      } catch (IOException e) {
        throw new RuntimeException("doRequest: Got IOException ", e);
      }
    }
  }

}
