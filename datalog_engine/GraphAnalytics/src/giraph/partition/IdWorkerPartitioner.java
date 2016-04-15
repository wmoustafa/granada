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

package giraph.partition;

import java.util.Collection;
import java.util.List;

import org.apache.giraph.partition.BasicPartitionOwner;
import org.apache.giraph.partition.PartitionBalancer;
import org.apache.giraph.partition.PartitionExchange;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.partition.PartitionStats;
import org.apache.giraph.partition.PartitionStore;
import org.apache.giraph.partition.WorkerGraphPartitioner;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.NullWritable;

import com.google.common.collect.Lists;

import giraph.SuperVertexId;
import schema.Database;

/**
 * Implements hash-based partitioning from the id hash code.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class IdWorkerPartitioner implements WorkerGraphPartitioner<SuperVertexId, Database, NullWritable> {
  /**
   * Mapping of the vertex ids to {@link PartitionOwner}.
   */
  protected List<PartitionOwner> partitionOwnerList =
      Lists.newArrayList();

  @Override
  public PartitionOwner createPartitionOwner() {
    return new BasicPartitionOwner();
  }

  @Override
  public PartitionOwner getPartitionOwner(SuperVertexId vertexId) {
    return partitionOwnerList.get(vertexId.getPartitionId());
  }

  @Override
  public Collection<PartitionStats> finalizePartitionStats(
      Collection<PartitionStats> workerPartitionStats,
      PartitionStore<SuperVertexId, Database, NullWritable> partitionStore) {
    // No modification necessary
    return workerPartitionStats;
  }

  @Override
  public PartitionExchange updatePartitionOwners(
      WorkerInfo myWorkerInfo,
      Collection<? extends PartitionOwner> masterSetPartitionOwners) {
    return PartitionBalancer.updatePartitionOwners(partitionOwnerList,
        myWorkerInfo, masterSetPartitionOwners);
  }

  @Override
  public Collection<? extends PartitionOwner> getPartitionOwners() {
    return partitionOwnerList;
  }
}
