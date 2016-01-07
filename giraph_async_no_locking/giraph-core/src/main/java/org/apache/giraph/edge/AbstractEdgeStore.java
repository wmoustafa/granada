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

package org.apache.giraph.edge;

import com.google.common.collect.MapMaker;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.utils.CallableFactory;
import org.apache.giraph.utils.ProgressableUtils;
import org.apache.giraph.utils.Trimmable;
import org.apache.giraph.utils.VertexIdEdgeIterator;
import org.apache.giraph.utils.VertexIdEdges;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;

/**
 * Basic implementation of edges store, extended this to easily define simple
 * and primitive edge stores
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <K> Key corresponding to Vertex id
 * @param <Et> Entry type
 */
public abstract class AbstractEdgeStore<I extends WritableComparable,
  V extends Writable, E extends Writable, K, Et>
  extends DefaultImmutableClassesGiraphConfigurable<I, V, E>
  implements EdgeStore<I, V, E> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(AbstractEdgeStore.class);
  /** Service worker. */
  protected CentralizedServiceWorker<I, V, E> service;
  /** Giraph configuration. */
  protected ImmutableClassesGiraphConfiguration<I, V, E> configuration;
  /** Progressable to report progress. */
  protected Progressable progressable;
  /** Map used to temporarily store incoming edges. */
  protected ConcurrentMap<Integer, Map<K, OutEdges<I, E>>> transientEdges;
  /**
   * Whether the chosen {@link OutEdges} implementation allows for Edge
   * reuse.
   */
  protected boolean reuseEdgeObjects;
  /**
   * Whether the {@link OutEdges} class used during input is different
   * from the one used during computation.
   */
  protected boolean useInputOutEdges;

  /**
   * Constructor.
   *
   * @param service Service worker
   * @param configuration Configuration
   * @param progressable Progressable
   */
  public AbstractEdgeStore(
    CentralizedServiceWorker<I, V, E> service,
    ImmutableClassesGiraphConfiguration<I, V, E> configuration,
    Progressable progressable) {
    this.service = service;
    this.configuration = configuration;
    this.progressable = progressable;
    transientEdges = new MapMaker().concurrencyLevel(
      configuration.getNettyServerExecutionConcurrency()).makeMap();
    reuseEdgeObjects = configuration.reuseEdgeObjects();
    useInputOutEdges = configuration.useInputOutEdges();
  }

  /**
   * Get vertexId for a given key
   *
   * @param entry for vertexId key
   * @param representativeVertexId representativeVertexId
   * @return vertex Id
   */
  protected abstract I getVertexId(Et entry, I representativeVertexId);

  /**
   * Create vertexId from a given key
   *
   * @param entry for vertexId key
   * @return new vertexId
   */
  protected abstract I createVertexId(Et entry);

  /**
   * Get OutEdges for a given partition
   *
   * @param partitionId id of partition
   * @return OutEdges for the partition
   */
  protected abstract Map<K, OutEdges<I, E>> getPartitionEdges(int partitionId);

  /**
   * Return the OutEdges for a given partition
   *
   * @param entry for vertexId key
   * @return out edges
   */
  protected abstract OutEdges<I, E> getPartitionEdges(Et entry);

  /**
   * Get iterator for partition edges
   *
   * @param partitionEdges map of out-edges for vertices in a partition
   * @return iterator
   */
  protected abstract Iterator<Et>
  getPartitionEdgesIterator(Map<K, OutEdges<I, E>> partitionEdges);

  /**
   * Get out-edges for a given vertex
   *
   * @param vertexIdEdgeIterator vertex Id Edge iterator
   * @param partitionEdgesIn map of out-edges for vertices in a partition
   * @return out-edges for the vertex
   */
  protected abstract OutEdges<I, E> getVertexOutEdges(
    VertexIdEdgeIterator<I, E> vertexIdEdgeIterator,
    Map<K, OutEdges<I, E>> partitionEdgesIn);

  @Override
  public void addPartitionEdges(
    int partitionId, VertexIdEdges<I, E> edges) {
    Map<K, OutEdges<I, E>> partitionEdges = getPartitionEdges(partitionId);

    VertexIdEdgeIterator<I, E> vertexIdEdgeIterator =
        edges.getVertexIdEdgeIterator();
    while (vertexIdEdgeIterator.hasNext()) {
      vertexIdEdgeIterator.next();
      Edge<I, E> edge = reuseEdgeObjects ?
          vertexIdEdgeIterator.getCurrentEdge() :
          vertexIdEdgeIterator.releaseCurrentEdge();
      OutEdges<I, E> outEdges = getVertexOutEdges(vertexIdEdgeIterator,
          partitionEdges);
      synchronized (outEdges) {
        outEdges.add(edge);
      }
    }
  }

  /**
   * Convert the input edges to the {@link OutEdges} data structure used
   * for computation (if different).
   *
   * @param inputEdges Input edges
   * @return Compute edges
   */
  private OutEdges<I, E> convertInputToComputeEdges(
    OutEdges<I, E> inputEdges) {
    if (!useInputOutEdges) {
      return inputEdges;
    } else {
      return configuration.createAndInitializeOutEdges(inputEdges);
    }
  }

  @Override
  public void moveEdgesToVertices() {
    final boolean createSourceVertex = configuration.getCreateSourceVertex();
    if (transientEdges.isEmpty()) {
      if (LOG.isInfoEnabled()) {
        LOG.info("moveEdgesToVertices: No edges to move");
      }
      return;
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("moveEdgesToVertices: Moving incoming edges to vertices.");
    }

    final BlockingQueue<Integer> partitionIdQueue =
        new ArrayBlockingQueue<>(transientEdges.size());
    partitionIdQueue.addAll(transientEdges.keySet());
    int numThreads = configuration.getNumInputSplitsThreads();

    CallableFactory<Void> callableFactory = new CallableFactory<Void>() {
      @Override
      public Callable<Void> newCallable(int callableId) {
        return new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            Integer partitionId;
            I representativeVertexId = configuration.createVertexId();
            while ((partitionId = partitionIdQueue.poll()) != null) {
              Partition<I, V, E> partition =
                  service.getPartitionStore().getOrCreatePartition(partitionId);
              Map<K, OutEdges<I, E>> partitionEdges =
                  transientEdges.remove(partitionId);
              Iterator<Et> iterator =
                  getPartitionEdgesIterator(partitionEdges);
              // process all vertices in given partition
              while (iterator.hasNext()) {
                Et entry = iterator.next();
                I vertexId = getVertexId(entry, representativeVertexId);
                OutEdges<I, E> outEdges = convertInputToComputeEdges(
                  getPartitionEdges(entry));
                Vertex<I, V, E> vertex = partition.getVertex(vertexId);
                // If the source vertex doesn't exist, create it. Otherwise,
                // just set the edges.
                if (vertex == null) {
                  if (createSourceVertex) {
                    // createVertex only if it is allowed by configuration
                    vertex = configuration.createVertex();
                    vertex.initialize(createVertexId(entry),
                        configuration.createVertexValue(), outEdges);
                    partition.putVertex(vertex);
                  }
                } else {
                  // A vertex may exist with or without edges initially
                  // and optimize the case of no initial edges
                  if (vertex.getNumEdges() == 0) {
                    vertex.setEdges(outEdges);
                  } else {
                    for (Edge<I, E> edge : outEdges) {
                      vertex.addEdge(edge);
                    }
                  }
                  if (vertex instanceof Trimmable) {
                    ((Trimmable) vertex).trim();
                  }
                  // Some Partition implementations (e.g. ByteArrayPartition)
                  // require us to put back the vertex after modifying it.
                  partition.saveVertex(vertex);
                }
                iterator.remove();
              }
              // Some PartitionStore implementations
              // (e.g. DiskBackedPartitionStore) require us to put back the
              // partition after modifying it.
              service.getPartitionStore().putPartition(partition);
            }
            return null;
          }
        };
      }
    };
    ProgressableUtils.getResultsWithNCallables(callableFactory, numThreads,
        "move-edges-%d", progressable);

    // remove all entries
    transientEdges.clear();

    if (LOG.isInfoEnabled()) {
      LOG.info("moveEdgesToVertices: Finished moving incoming edges to " +
          "vertices.");
    }
  }
}
