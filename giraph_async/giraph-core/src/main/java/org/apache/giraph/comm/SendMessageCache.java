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

package org.apache.giraph.comm;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.messages.primitives.long_id.SenderIdIterationNumber;
import org.apache.giraph.comm.netty.NettyWorkerClientRequestProcessor;
import org.apache.giraph.comm.requests.SendWorkerMessagesRequest;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.PairList;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.apache.giraph.conf.GiraphConstants.ADDITIONAL_MSG_REQUEST_SIZE;
import static org.apache.giraph.conf.GiraphConstants.MAX_MSG_REQUEST_SIZE;

/**
 * Aggregates the messages to be sent to workers so they can be sent
 * in bulk.  Not thread-safe.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
@SuppressWarnings("unchecked")
public class SendMessageCache<I extends WritableComparable, M extends Writable>
    extends SendVertexIdDataCache<I, M, VertexIdMessages<I, M>> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(SendMessageCache.class);
  /** Messages sent during the last superstep */
  protected long totalMsgsSentInSuperstep = 0;
  /** Message bytes sent during the last superstep */
  protected long totalMsgBytesSentInSuperstep = 0;
  /*Vicky*/
  protected long totalAddMessageTimeInSuperstep = 0;
  protected long totalAddMessageCounterInSuperstep = 0;

  /** Max message size sent to a worker */
  protected final int maxMessagesSizePerWorker;
  /** NettyWorkerClientRequestProcessor for message sending */
  protected final NettyWorkerClientRequestProcessor<I, ?, ?> clientProcessor;

  private ImmutableClassesGiraphConfiguration giraph_conf;
  /**
   * Constructor
   *
   * @param conf Giraph configuration
   * @param serviceWorker Service worker
   * @param processor NettyWorkerClientRequestProcessor
   * @param maxMsgSize Max message size sent to a worker
   */
  public SendMessageCache(ImmutableClassesGiraphConfiguration conf,
      CentralizedServiceWorker<?, ?, ?> serviceWorker,
      NettyWorkerClientRequestProcessor<I, ?, ?> processor,
      int maxMsgSize) {
    super(conf, serviceWorker, MAX_MSG_REQUEST_SIZE.get(conf),
        ADDITIONAL_MSG_REQUEST_SIZE.get(conf));
    maxMessagesSizePerWorker = maxMsgSize;
    clientProcessor = processor;
    giraph_conf = conf;

  }

  @Override
  public VertexIdMessages<I, M> createVertexIdData() {
    return new ByteArrayVertexIdMessages<I, M>(
        getConf().<M>createOutgoingMessageValueFactory());
  }

  /**
   * Add a message to the cache.
   *
   * @param workerInfo the remote worker destination
   * @param partitionId the remote Partition this message belongs to
   * @param destVertexId vertex id that is ultimate destination
   * @param message Message to send to remote worker
   * @return Size of messages for the worker.
   */
  public int addMessage(WorkerInfo workerInfo,
                        int partitionId, I destVertexId, M message) {

    return addData(workerInfo, partitionId, destVertexId, message);
  }

  /**
   * Add a message to the cache with serialized ids.
   *
   * @param workerInfo The remote worker destination
   * @param partitionId The remote Partition this message belongs to
   * @param serializedId Serialized vertex id that is ultimate destination
   * @param idSerializerPos The end position of serialized id in the byte array
   * @param message Message to send to remote worker
   * @return Size of messages for the worker.
   */
  protected int addMessage(WorkerInfo workerInfo, int partitionId,
      byte[] serializedId, int idSerializerPos, M message) {
    return addData(
            workerInfo, partitionId, serializedId,
            idSerializerPos, message);
  }

  /**
   * Gets the messages for a worker and removes it from the cache.
   *
   * @param workerInfo the address of the worker who owns the data
   *                   partitions that are receiving the messages
   * @return List of pairs (partitionId, ByteArrayVertexIdMessages),
   *         where all partition ids belong to workerInfo
   */
  protected PairList<Integer, VertexIdMessages<I, M>>
  removeWorkerMessages(WorkerInfo workerInfo) {
    return removeWorkerData(workerInfo);
  }

  /**
   * Gets all the messages and removes them from the cache.
   *
   * @return All vertex messages for all partitions
   */
  private PairList<WorkerInfo, PairList<
      Integer, VertexIdMessages<I, M>>> removeAllMessages() {
    return removeAllData();
  }

  /**
   * Send a message to a target vertex id.
   *
   * @param destVertexId Target vertex id
   * @param message The message sent to the target
   */
  public void sendMessageRequest(I destVertexId, M message) {

    PartitionOwner owner =
      getServiceWorker().getVertexPartitionOwner(destVertexId);
    WorkerInfo workerInfo = owner.getWorkerInfo();
    final int partitionId = owner.getPartitionId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("sendMessageRequest: Send bytes (" + message.toString() +
            ") to " + destVertexId + " on partition " + partitionId + "on worker " + workerInfo);
    }
    ++totalMsgsSentInSuperstep;

    // Add the message to the cache
    int workerMessageSize = addMessage(
      workerInfo, partitionId, destVertexId, message);
    // Send a request if the cache of outgoing message to
    // the remote worker 'workerInfo' is full enough to be flushed
    if (workerMessageSize >= maxMessagesSizePerWorker) {
      if(LOG.isDebugEnabled())
        LOG.debug("Vicky --> Flush send data cache. Current size = " + workerMessageSize + " , Max size = " + maxMessagesSizePerWorker);
      PairList<Integer, VertexIdMessages<I, M>>
        workerMessages = removeWorkerMessages(workerInfo);
      WritableRequest writableRequest =
        new SendWorkerMessagesRequest<I, M>(workerMessages);
      totalMsgBytesSentInSuperstep += writableRequest.getSerializedSize();
      clientProcessor.doRequest(workerInfo, writableRequest);
      totalAddMessageTimeInSuperstep += ((SendWorkerMessagesRequest)writableRequest).getAddMessageTime();
      totalAddMessageCounterInSuperstep += ((SendWorkerMessagesRequest)writableRequest).getAddMessageCounter();
      // Notify sending
      getServiceWorker().getGraphTaskManager().notifySentMessages();
    }
  }



  public void sendSourceMessageRequest(Vertex<I, ?, ?> vertex, I destVertexId, M message) {

    LOG.debug("Vicky --> :sendSourceMessageRequest Create sender info");
    TargetVertexIdIterator targetVertexIterator =
            new TargetVertexIdIterator(vertex);

    PartitionOwner owner =
            getServiceWorker().getVertexPartitionOwner(vertex.getId());
    final int partitionId = owner.getPartitionId();

    SenderIdIterationNumber sender = new SenderIdIterationNumber(vertex.getId(),
            clientProcessor.getLocalSuperstepOfPartition(partitionId));

    //Find all neighbor ids
    //TODO Vicky: is there a better way?
    Set<LongWritable> vids = new HashSet<LongWritable>();
    while (targetVertexIterator.hasNext()) {
      LongWritable vid = new LongWritable(((LongWritable)targetVertexIterator.next()).get());
      vids.add(vid);
    }
    LOG.debug("Vicky --> sendSourceMessageToAllRequest: neighbors = " + vids);
    sender.setNeighbors(vids);
    sender.initialize_map();

    sendExtendedMessageRequest(sender, destVertexId,message);
  }

  /**
   * Vicky
   * Send a message to a target vertex id.
   *
   * @param destVertexId Target vertex id
   * @param message The message sent to the target
   */
  public void sendExtendedMessageRequest(SenderIdIterationNumber sender, I destVertexId, M message) {


    PartitionOwner owner =
            getServiceWorker().getVertexPartitionOwner(destVertexId);
    WorkerInfo workerInfo = owner.getWorkerInfo();
    final int partitionId = owner.getPartitionId();
    LOG.debug("Vicky --> sendExtendedMessageRequest: Send message to vertex " + destVertexId + " on partition " + partitionId);
    //LOG.info("sendExtendedMessageRequest: Send bytes (" + message.toString() +
    //        ") to " + destVertexId + " on worker " + workerInfo);
    ++totalMsgsSentInSuperstep;

    //Vicky Send one message immediately without putting it in cache

    //TODO Buffer messages instead of sending one by one
    VertexIdMessages<I, M> v_data = createVertexIdData();
    v_data.setConf(getConf());
    v_data.initialize(); //create extendedDataOutput
    ((ByteArrayVertexIdMessages)v_data).add(destVertexId, message); // write to extendedDataOutput
    // PairList both lists must have same size
    //Workaround: Create a pair of pID and sender Info
    PairList<Integer,SenderIdIterationNumber> workaround = new PairList<Integer,SenderIdIterationNumber>();
    workaround.initialize(1);
    workaround.add(partitionId,sender);


    WritableRequest wRequest =
            new SendWorkerMessagesRequest<I, M>(workaround, v_data);
    totalMsgBytesSentInSuperstep += wRequest.getSerializedSize();
    clientProcessor.doRequest(workerInfo, wRequest);
    // Notify sending
    getServiceWorker().getGraphTaskManager().notifySentMessages();
  }


  /**
   * An iterator wrapper on edges to return
   * target vertex ids.
   */
  public static class TargetVertexIdIterator<I extends WritableComparable>
      implements Iterator<I> {
    /** An edge iterator */
    private final Iterator<Edge<I, Writable>> edgesIterator;

    /**
     * Constructor.
     *
     * @param vertex The source vertex of the out edges
     */
    public TargetVertexIdIterator(Vertex<I, ?, ?> vertex) {
      edgesIterator =
        ((Vertex<I, Writable, Writable>) vertex).getEdges().iterator();
    }

    @Override
    public boolean hasNext() {
      return edgesIterator.hasNext();
    }

    @Override
    public I next() {
      return edgesIterator.next().getTargetVertexId();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Send message to all its neighbors
   *
   * @param vertex The source vertex
   * @param message The message sent to a worker
   */
  public void sendMessageToAllRequest(Vertex<I, ?, ?> vertex, M message) {
    TargetVertexIdIterator targetVertexIterator =
      new TargetVertexIdIterator(vertex);

    PartitionOwner owner =
            getServiceWorker().getVertexPartitionOwner(vertex.getId());
    final int partitionId = owner.getPartitionId();

    SenderIdIterationNumber sender = new SenderIdIterationNumber(vertex.getId(),
            clientProcessor.getLocalSuperstepOfPartition(partitionId));

    //Vicky
    //sendMessageToAllRequest(targetVertexIterator, message);
    sendExtendedMessageToAllRequest(sender, targetVertexIterator, message);
  }

  /**
   * Vicky
   * Send message to the target ids in the iterator
   *
   * @param vertexIdIterator The iterator of target vertex ids
   * @param message The message sent to a worker
   */
  public void sendExtendedMessageToAllRequest(SenderIdIterationNumber sender, Iterator<I> vertexIdIterator, M message) {

    Set<LongWritable> vids = new HashSet<LongWritable>();
    while (vertexIdIterator.hasNext()) {

      //I vid = (I)ReflectionUtils.newInstance(giraph_conf.getVertexIdClass(), giraph_conf);
      //LOG.debug("Vicky vertex id class = " + giraph_conf.getVertexIdClass().getSimpleName());
      LongWritable vid = new LongWritable(((LongWritable)vertexIdIterator.next()).get());
      vids.add(vid);

    }
    LOG.info("sendExtendedMessageToAllRequest: neighbors = " + vids);
    sender.setNeighbors(vids);
    sender.initialize_map();
    LOG.info("sendExtendedMessageToAllRequest: sender info when sending new messages for sender " + sender);
    for(LongWritable vid: vids) {
      //Vicky
      sendExtendedMessageRequest(sender, (I)vid, message);
    }

  }


  /**
   * Vicky
   * Send message to the target ids in the iterator
   *
   * @param vertexIdIterator The iterator of target vertex ids
   * @param message The message sent to a worker
   */
  public void sendMessageToAllRequest(Iterator<I> vertexIdIterator, M message) {
    while (vertexIdIterator.hasNext()) {

      sendMessageRequest(vertexIdIterator.next(), message);
    }
  }

  /**
   * Flush the rest of the messages to the workers.
   */
  public void flush() {

    PairList<WorkerInfo, PairList<Integer,
        VertexIdMessages<I, M>>>
    remainingMessageCache = removeAllMessages();
    PairList<WorkerInfo, PairList<
        Integer, VertexIdMessages<I, M>>>.Iterator
    iterator = remainingMessageCache.getIterator();
    while (iterator.hasNext()) {
      iterator.next();
      WritableRequest writableRequest =
        new SendWorkerMessagesRequest<I, M>(
          iterator.getCurrentSecond());
      totalMsgBytesSentInSuperstep += writableRequest.getSerializedSize();
      clientProcessor.doRequest(
              iterator.getCurrentFirst(), writableRequest);
    }
  }

  /**
   * Reset the message count per superstep.
   *
   * @return The message count sent in last superstep
   */
  public long resetMessageCount() {
    long messagesSentInSuperstep = totalMsgsSentInSuperstep;
    totalMsgsSentInSuperstep = 0;
    return messagesSentInSuperstep;
  }

  /**
   * Reset the message bytes count per superstep.
   *
   * @return The message count sent in last superstep
   */
  public long resetMessageBytesCount() {
    long messageBytesSentInSuperstep = totalMsgBytesSentInSuperstep;
    totalMsgBytesSentInSuperstep = 0;
    return messageBytesSentInSuperstep;
  }

  public long resetAddMessageTime() {
    long addMessageTime = totalAddMessageTimeInSuperstep;
    totalAddMessageTimeInSuperstep = 0;

    return addMessageTime;
  }

  public long resetAddMessageCounter() {
    long addMessageCounter = totalAddMessageCounterInSuperstep;
    totalAddMessageCounterInSuperstep = 0;

    return addMessageCounter;
  }

}
