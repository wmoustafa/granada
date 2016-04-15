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

import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.partition.GraphPartitionerFactory;
import org.apache.giraph.partition.MasterGraphPartitioner;
import org.apache.giraph.partition.WorkerGraphPartitioner;
import org.apache.giraph.worker.LocalData;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import giraph.SuperVertexId;
import schema.Database;

/**
 * Divides the vertices into partitions by their hash code using a simple
 * round-robin hash for great balancing if given a random hash code.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class IdPartitionerFactory
  extends DefaultImmutableClassesGiraphConfigurable<SuperVertexId, Database, NullWritable>
  implements GraphPartitionerFactory<SuperVertexId, Database, NullWritable>  {

  @Override
  public void initialize(LocalData<SuperVertexId, Database, NullWritable, ? extends Writable> localData) {
  }

  @Override
  public MasterGraphPartitioner<SuperVertexId, Database, NullWritable> createMasterGraphPartitioner() {
    return new IdMasterPartitioner(getConf());
  }

  @Override
  public WorkerGraphPartitioner<SuperVertexId, Database, NullWritable> createWorkerGraphPartitioner() {
    return new IdWorkerPartitioner();
  }
}
