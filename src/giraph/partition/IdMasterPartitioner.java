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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.partition.BasicPartitionOwner;
import org.apache.giraph.partition.MasterGraphPartitioner;
import org.apache.giraph.partition.PartitionBalancer;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.partition.PartitionStats;
import org.apache.giraph.partition.PartitionUtils;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import giraph.SuperVertexId;
import schema.Database;

/**
 * Master will execute a hash based partitioning.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class IdMasterPartitioner implements
MasterGraphPartitioner<SuperVertexId, Database, NullWritable> {
	/** Class logger */
	private static Logger LOG = Logger.getLogger(IdMasterPartitioner.class);
	/** Provided configuration */
	private ImmutableClassesGiraphConfiguration conf;
	/** Save the last generated partition owner list */
	private List<PartitionOwner> partitionOwnerList;

	/**
	 * Constructor.
	 *
	 *@param conf Configuration used.
	 */
	public IdMasterPartitioner(ImmutableClassesGiraphConfiguration conf) {
		this.conf = conf;
		//this.conf.set(PartitionBalancer.PARTITION_BALANCE_ALGORITHM, PartitionBalancer.EGDE_BALANCE_ALGORITHM);
	}

	@Override
	public Collection<PartitionOwner> createInitialPartitionOwners(
			Collection<WorkerInfo> availableWorkerInfos, int maxWorkers) {
		int partitionCount = PartitionUtils.computePartitionCount(
				availableWorkerInfos, maxWorkers, conf);
		
		Map<Integer, WorkerInfo> partitionOwnerMap = getPartitionOwnerMap(availableWorkerInfos, partitionCount);
		PartitionOwner[] ownerArray = new PartitionOwner[partitionOwnerMap.size()];

		for (Entry<Integer, WorkerInfo> e : partitionOwnerMap.entrySet())
		{
			PartitionOwner owner = new BasicPartitionOwner(e.getKey(), e.getValue());
			ownerArray[e.getKey()] = owner;
		}
		
		this.partitionOwnerList = new ArrayList<>();
		for (PartitionOwner owner : ownerArray)
			this.partitionOwnerList.add(owner);
		return this.partitionOwnerList;
	}

	//Vicky FIXME removed override
	public void setPartitionOwners(Collection<PartitionOwner> partitionOwners) {
		this.partitionOwnerList = Lists.newArrayList(partitionOwners);
	}

	@Override
	public Collection<PartitionOwner> getCurrentPartitionOwners() {
		return partitionOwnerList;
	}

	/**
	 * Subclasses can set the partition owner list.
	 *
	 * @param partitionOwnerList New partition owner list.
	 */
	protected void setPartitionOwnerList(List<PartitionOwner>
	partitionOwnerList) {
		this.partitionOwnerList = partitionOwnerList;
	}

	@Override
	public Collection<PartitionOwner> generateChangedPartitionOwners(
			Collection<PartitionStats> allPartitionStatsList,
			Collection<WorkerInfo> availableWorkerInfos,
			int maxWorkers,
			long superstep) {
		return PartitionBalancer.balancePartitionsAcrossWorkers(
				conf,
				partitionOwnerList,
				allPartitionStatsList,
				availableWorkerInfos);
	}

	@Override
	public PartitionStats createPartitionStats() {
		return new PartitionStats();
	}

	Map<Integer, WorkerInfo> getPartitionOwnerMap(Collection<WorkerInfo> availableWorkerInfos, int partitionCount)
	{
		
		Map<Integer, WorkerInfo> partitionOwnerNumbers = new HashMap<>();
		int partitionOwnerNumber = 0;
		for (WorkerInfo worker : availableWorkerInfos)
		{
			partitionOwnerNumbers.put(partitionOwnerNumber, worker);
			partitionOwnerNumber++;
		}
		
		int workerCount = availableWorkerInfos.size();
		int freq = ((partitionCount - 1) / workerCount) + 1;
		Map<Integer, WorkerInfo> partitionOwnerMap = new HashMap<Integer, WorkerInfo>();
		for (int i = 0; i < partitionCount; i++)
			partitionOwnerMap.put(i, partitionOwnerNumbers.get(i/workerCount/freq));
		/*try
		{
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream in = fs.open(new Path("hdfs://yoda2.nec-labs.com:8020/user/walaa/input/smallworld10k.txt.partition.owners"));
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line;
			while ((line = br.readLine()) != null)
			{
				String[] lineSplits = line.split("\\s+");
				int partitionId = Integer.parseInt(lineSplits[0]);
				int workerId = Integer.parseInt(lineSplits[1]);
				partitionOwnerMap.put(partitionId, partitionOwnerNumbers.get(workerId));
			}

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}*/
		
		return partitionOwnerMap;
	}


}
