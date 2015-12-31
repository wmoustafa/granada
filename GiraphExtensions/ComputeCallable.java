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
package org.apache.giraph.graph;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.comm.messages.ByteArrayMessagesPerVertexStore;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.comm.messages.OneMessagePerVertexStore;
import org.apache.giraph.comm.messages.SimpleMessageStore;
import org.apache.giraph.comm.netty.NettyWorkerClientRequestProcessor;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.io.SimpleVertexWriter;
import org.apache.giraph.metrics.GiraphMetrics;
import org.apache.giraph.metrics.MetricNames;
import org.apache.giraph.metrics.SuperstepMetricsRegistry;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.PartitionStats;
import org.apache.giraph.time.SystemTime;
import org.apache.giraph.time.Time;
import org.apache.giraph.time.Times;
import org.apache.giraph.utils.EmptyIterable;
import org.apache.giraph.utils.MemoryUtils;
import org.apache.giraph.utils.TimedLogger;
import org.apache.giraph.utils.Trimmable;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerProgress;
import org.apache.giraph.worker.WorkerThreadGlobalCommUsage;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

/**
 * Compute as many vertex partitions as possible.  Every thread will has its
 * own instance of WorkerClientRequestProcessor to send requests.  Note that
 * the partition ids are used in the partitionIdQueue rather than the actual
 * partitions since that would cause the partitions to be loaded into memory
 * when using the out-of-core graph partition store.  We should only load on
 * demand.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M1> Incoming message type
 * @param <M2> Outgoing message type
 */
public class ComputeCallable<I extends WritableComparable, V extends Writable,
    E extends Writable, M1 extends Writable, M2 extends Writable>
    implements Callable<Collection<PartitionStats>> {
  /** Class logger */
  private static final Logger LOG  = Logger.getLogger(ComputeCallable.class);
  /** Class time object */
  private static final Time TIME = SystemTime.get();
  /** How often to update WorkerProgress */
  private static final long VERTICES_TO_UPDATE_PROGRESS = 100000;
  /** Context */
  private final Mapper<?, ?, ?, ?>.Context context;
  /** Graph state */
  private final GraphState graphState;
  /** Thread-safe queue of all partition ids */
  private final BlockingQueue<Integer> partitionIdQueue;
  /** Message store */
  private final MessageStore<I, M1> messageStore;

  //Vicky
  /** Message store */
  private final MessageStore<I, M1> incoming_messageStore;
  private Map<Integer,Long> local_supersteps;
  private final long global_superstep;
  private  WorkerClientRequestProcessor<I, V, E> workerClientRequestProcessor;

  /** Configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E> configuration;
  /** Worker (for NettyWorkerClientRequestProcessor) */
  private final CentralizedServiceWorker<I, V, E> serviceWorker;
  /** Dump some progress every 30 seconds */
  private final TimedLogger timedLogger = new TimedLogger(30 * 1000, LOG);
  /** VertexWriter for this ComputeCallable */
  private SimpleVertexWriter<I, V, E> vertexWriter;
  /** Get the start time in nanos */
  private final long startNanos = TIME.getNanoseconds();

  // Per-Superstep Metrics
  /** Messages sent */
  private final Counter messagesSentCounter;
  /** Message bytes sent */
  private final Counter messageBytesSentCounter;
  /** Compute time per partition */
  private final Histogram histogramComputePerPartition;

  private long compute_incovations_counter = 0;
  private long getMessageTime = 0;
  private long orderVerticesTime = 0;
  private long getMessageCounter = 0;

  /**
   * Constructor
   *  @param context Context
   * @param graphState Current graph state (use to create own graph state)
   * @param messageStore Message store
   * @param partitionIdQueue Queue of partition ids (thread-safe)
   * @param configuration Configuration
   * @param serviceWorker Service worker
   * @param global_superstep
   */
  public ComputeCallable(
          Mapper<?, ?, ?, ?>.Context context, GraphState graphState,
          MessageStore<I, M1> messageStore,
          MessageStore<I, M1> incoming_messageStore,
          BlockingQueue<Integer> partitionIdQueue,
          ImmutableClassesGiraphConfiguration<I, V, E> configuration,
          CentralizedServiceWorker<I, V, E> serviceWorker,
          Map<Integer, Long> local_supersteps, long global_superstep) { // Vicky: Added local_supersteps variable
    this.context = context;
    this.configuration = configuration;
    this.partitionIdQueue = partitionIdQueue;
    this.messageStore = messageStore;
    this.incoming_messageStore = incoming_messageStore;
    this.serviceWorker = serviceWorker;
    this.graphState = graphState;
    this.local_supersteps = local_supersteps;
    this.global_superstep = global_superstep;

    SuperstepMetricsRegistry metrics = GiraphMetrics.get().perSuperstep();
    messagesSentCounter = metrics.getCounter(MetricNames.MESSAGES_SENT);
    messageBytesSentCounter =
      metrics.getCounter(MetricNames.MESSAGE_BYTES_SENT);
    histogramComputePerPartition = metrics.getUniformHistogram(
        MetricNames.HISTOGRAM_COMPUTE_PER_PARTITION);
  }

  @Override
  public Collection<PartitionStats> call() {
    // Thread initialization (for locality)


    workerClientRequestProcessor =
        new NettyWorkerClientRequestProcessor<I, V, E>(
            context, configuration, serviceWorker, local_supersteps, //Vicky added parameter local_supersteps
            configuration.getOutgoingMessageEncodeAndStoreType().
              useOneMessageToManyIdsEncoding());
    WorkerThreadGlobalCommUsage aggregatorUsage =
        serviceWorker.getAggregatorHandler().newThreadAggregatorUsage();
    WorkerContext workerContext = serviceWorker.getWorkerContext();

    vertexWriter = serviceWorker.getSuperstepOutput().getVertexWriter();

    Computation<I, V, E, M1, M2> computation =
        (Computation<I, V, E, M1, M2>) configuration.createComputation();
    computation.initialize(graphState, workerClientRequestProcessor,
        serviceWorker.getGraphTaskManager(), aggregatorUsage, workerContext);
    computation.preSuperstep();

    List<PartitionStats> partitionStatsList = Lists.newArrayList();
    long message_counter = 0;

    while (!partitionIdQueue.isEmpty()) {
      Integer partitionId = partitionIdQueue.poll();
      if (partitionId == null) {
        break;
      }

      long startTime = System.currentTimeMillis();
      Partition<I, V, E> partition =
          serviceWorker.getPartitionStore().getOrCreatePartition(partitionId);

      try {
        serviceWorker.getServerData().resolvePartitionMutation(partition);
        PartitionStats partitionStats = computePartitionDatalogWhile(computation, partition);
        partitionStatsList.add(partitionStats);

        // Gather statistics per partition
        long partitionMsgs = workerClientRequestProcessor.resetMessageCount();
        partitionStats.addMessagesSentCount(partitionMsgs);
        messagesSentCounter.inc(partitionMsgs);
        message_counter+=partitionMsgs;

        long partitionMsgBytes =
          workerClientRequestProcessor.resetMessageBytesCount();
        partitionStats.addMessageBytesSentCount(partitionMsgBytes);
        messageBytesSentCounter.inc(partitionMsgBytes);

        long addMessageTime =
                ((NettyWorkerClientRequestProcessor)workerClientRequestProcessor).resetAddMessageTime();
        partitionStats.addAddMessageTime(addMessageTime);
        partitionStats.addGetMessageTime(getMessageTime);
        partitionStats.addComputeInvocations(compute_incovations_counter);
        partitionStats.addOrderVertices(orderVerticesTime);
        long addMessageCounter =
                ((NettyWorkerClientRequestProcessor)workerClientRequestProcessor).resetAddMessageCounter();
        partitionStats.addAddMessageCounter(addMessageCounter);
        partitionStats.addGetMessageCounter(getMessageCounter);


        timedLogger.info("call: Completed " +
                partitionStatsList.size() + " partitions, " +
                partitionIdQueue.size() + " remaining " +
                MemoryUtils.getRuntimeMemoryStats());
      } catch (IOException e) {
        throw new IllegalStateException("call: Caught unexpected IOException," +
            " failing.", e);
      } catch (InterruptedException e) {
        throw new IllegalStateException("call: Caught unexpected " +
            "InterruptedException, failing.", e);
      } finally {
        serviceWorker.getPartitionStore().putPartition(partition);
      }

      histogramComputePerPartition.update(
        System.currentTimeMillis() - startTime);
    }

    computation.postSuperstep();

    // Return VertexWriter after the usage
    serviceWorker.getSuperstepOutput().returnVertexWriter(vertexWriter);

    if (LOG.isInfoEnabled()) {
      float seconds = Times.getNanosSince(TIME, startNanos) /
          Time.NS_PER_SECOND_AS_FLOAT;
      LOG.info("call: Computation took " + seconds + " secs for "  +
          partitionStatsList.size() + " partitions on superstep " +
          graphState.getSuperstep() + ".  Flushing started");
    }
    try {
      workerClientRequestProcessor.flush();
      // The messages flushed out from the cache is
      // from the last partition processed
      if (partitionStatsList.size() > 0) {
        long partitionMsgBytes =
          workerClientRequestProcessor.resetMessageBytesCount();
        partitionStatsList.get(partitionStatsList.size() - 1).
          addMessageBytesSentCount(partitionMsgBytes);
        messageBytesSentCounter.inc(partitionMsgBytes);
      }
      aggregatorUsage.finishThreadComputation();
    } catch (IOException e) {
      throw new IllegalStateException("call: Flushing failed.", e);
    }
    return partitionStatsList;
  }



  /**
   * Compute a single partition
   *
   * @param computation Computation to use
   * @param partition Partition to compute
   * @return Partition stats for this computed partition
   */
  private PartitionStats computePartitionDatalog(Computation<I, V, E, M1, M2> computation, Partition<I, V, E> partition)
          throws IOException, InterruptedException {

    PartitionStats partitionStats = new PartitionStats(partition.getId(), 0, 0, 0, 0, 0);
    long verticesComputedProgress = 0;

    // Make sure this is thread-safe across runs
    synchronized (partition) {

      if(global_superstep == 0) {
        computPartitionSuperstep0(computation, partition,partitionStats, verticesComputedProgress);
      }
      else {
          computPartitionForLoopDatalog(computation, partition, partitionStats, verticesComputedProgress);
      }
    }
    WorkerProgress.get().addVerticesComputed(verticesComputedProgress);
    WorkerProgress.get().incrementPartitionsComputed();
    return partitionStats;
  }



  /**
   * Compute a single partition using a while loop for Datalog
   *
   * @param computation Computation to use
   * @param partition Partition to compute
   * @return Partition stats for this computed partition
   */
  private PartitionStats computePartitionDatalogWhile(Computation<I, V, E, M1, M2> computation, Partition<I, V, E> partition)
          throws IOException, InterruptedException {


    PartitionStats partitionStats = new PartitionStats(partition.getId(), 0, 0, 0, 0, 0);
    long verticesComputedProgress = 0;

    // Make sure this is thread-safe across runs
    synchronized (partition) {

      if(global_superstep == 0) {
        computPartitionSuperstep0(computation, partition,partitionStats, verticesComputedProgress);
      }
      else {
        while (((SimpleMessageStore) incoming_messageStore).get_has_messages_map(partition.getId()).size() > 0) {
          computPartitionForLoopDatalogWhile(computation, partition, partitionStats, verticesComputedProgress);
        }
      }
    }
    WorkerProgress.get().addVerticesComputed(verticesComputedProgress);
    WorkerProgress.get().incrementPartitionsComputed();
    return partitionStats;
  }



  /**
   * Compute a single partition
   *
   * @param computation Computation to use
   * @param partition Partition to compute
   */
  private void computPartitionForLoopDatalog(Computation<I, V, E, M1, M2> computation, Partition<I, V, E> partition,
                                      PartitionStats partitionStats, long verticesComputedProgress)
          throws IOException, InterruptedException {

    for (Vertex<I, V, E> vertex : partition) {

      long start = System.currentTimeMillis();
      Iterable<M1> messages = null;
      if(configuration.useOutgoingMessageCombiner() == true)
        messages = ((OneMessagePerVertexStore) incoming_messageStore).getVertexMessagesImproved(vertex.getId(), partition.getId()); //When combiner is used
      else
        messages = ((ByteArrayMessagesPerVertexStore) incoming_messageStore).getVertexMessagesImproved(vertex.getId(), partition.getId()); //Without combiner
      long end = System.currentTimeMillis();

      getMessageTime += (end-start);
      getMessageCounter++;
      
      if (vertex.isHalted() && !Iterables.isEmpty(messages)) {
        if(vertex.isHalted())
        vertex.wakeUp();
      }
      if (!vertex.isHalted()) {
        context.progress();

        //------------------------------------------------------------
        computation.compute(vertex, messages);
        compute_incovations_counter++;
        //------------------------------------------------------------
        vertex.voteToHalt();
        // Need to unwrap the mutated edges (possibly)
        vertex.unwrapMutableEdges();
        //Compact edges representation if possible
        if (vertex instanceof Trimmable) {
          ((Trimmable) vertex).trim();
        }
        // Write vertex to superstep output (no-op if it is not used)
        vertexWriter.writeVertex(vertex);
        // Need to save the vertex changes (possibly)
        partition.saveVertex(vertex);


      }
      if (vertex.isHalted()) {
        partitionStats.incrFinishedVertexCount();
      }
      // Add statistics for this vertex
      partitionStats.incrVertexCount();
      partitionStats.addEdgeCount(vertex.getNumEdges());

      verticesComputedProgress++;
      if (verticesComputedProgress == VERTICES_TO_UPDATE_PROGRESS) {
        WorkerProgress.get().addVerticesComputed(verticesComputedProgress);
        verticesComputedProgress = 0;
      }

    } //end of for
  }

  /**
   * Compute a single partition
   *
   * @param computation Computation to use
   * @param partition Partition to compute
   */
  private void computPartitionForLoopDatalogWhile(Computation<I, V, E, M1, M2> computation, Partition<I, V, E> partition,
                                      PartitionStats partitionStats, long verticesComputedProgress)
          throws IOException, InterruptedException {

    for (Vertex<I, V, E> vertex : partition) {

    //TODO Smart for loop not working!
    //for(Object vertex_id: ((SimpleMessageStore) incoming_messageStore).get_has_messages_map(partition.getId()).keySet()) {
    //  Vertex<I, V, E> vertex = partition.getVertex((I)vertex_id);

      //LOG.info("Vicky ---> Current vertex=" + vertex.getId() + ":" + vertex.getValue() + ", getVertexMessages at superstep "
      //        + graphState.getSuperstep());

      long start = System.currentTimeMillis();
      Iterable<M1> messages = null;
      if(configuration.useOutgoingMessageCombiner() == true)
        messages = ((OneMessagePerVertexStore) incoming_messageStore).getVertexMessagesImproved(vertex.getId(), partition.getId()); //When combiner is used
      else
        messages = ((ByteArrayMessagesPerVertexStore) incoming_messageStore).getVertexMessagesImproved(vertex.getId(), partition.getId()); //Without combiner

      long end = System.currentTimeMillis();

      getMessageTime += (end-start);
      getMessageCounter++;

      if (vertex.isHalted() && !Iterables.isEmpty(messages)) {
        if(vertex.isHalted())
          vertex.wakeUp();
      }
      if (!vertex.isHalted()) {
        context.progress();
        //------------------------------------------------------------
        computation.compute(vertex, messages);
        compute_incovations_counter++;
        //------------------------------------------------------------
        vertex.voteToHalt();
        // Need to unwrap the mutated edges (possibly)
        vertex.unwrapMutableEdges();
        //Compact edges representation if possible
        if (vertex instanceof Trimmable) {
          ((Trimmable) vertex).trim();
        }
        // Write vertex to superstep output (no-op if it is not used)
        vertexWriter.writeVertex(vertex);
        // Need to save the vertex changes (possibly)
        partition.saveVertex(vertex);


      }
      if (vertex.isHalted()) {
        partitionStats.incrFinishedVertexCount();
      }
      // Add statistics for this vertex
      partitionStats.incrVertexCount();
      partitionStats.addEdgeCount(vertex.getNumEdges());

      verticesComputedProgress++;
      if (verticesComputedProgress == VERTICES_TO_UPDATE_PROGRESS) {
        WorkerProgress.get().addVerticesComputed(verticesComputedProgress);
        verticesComputedProgress = 0;
      }

    } //end of for
  }


  /**
   * Compute a single partition
   *
   * @param computation Computation to use
   * @param partition Partition to compute
   */
  private void computPartitionSuperstep0(Computation<I, V, E, M1, M2> computation, Partition<I, V, E> partition,
                                         PartitionStats partitionStats, long verticesComputedProgress)
          throws IOException, InterruptedException {
    
    for (Vertex<I, V, E> vertex : partition) {

      Iterable<M1> messages = new EmptyIterable<M1>();

      if (vertex.isHalted() && !Iterables.isEmpty(messages)) {
        vertex.wakeUp();
      }
      if (!vertex.isHalted()) {
        context.progress();
        //------------------------------------------------------------
        computation.compute(vertex, messages);
        compute_incovations_counter++;
        //------------------------------------------------------------
        vertex.voteToHalt();

      }
      if (vertex.isHalted()) {
        partitionStats.incrFinishedVertexCount();
      }
      // Add statistics for this vertex
      partitionStats.incrVertexCount();
      partitionStats.addEdgeCount(vertex.getNumEdges());

      verticesComputedProgress++;
      if (verticesComputedProgress == VERTICES_TO_UPDATE_PROGRESS) {
        WorkerProgress.get().addVerticesComputed(verticesComputedProgress);
        verticesComputedProgress = 0;
      }

    } //end of for
  }

}

