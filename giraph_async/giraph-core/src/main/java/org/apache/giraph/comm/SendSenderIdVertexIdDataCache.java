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

/**
 * Created by Vicky Papavasileiou on 7/30/15.
 */

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.messages.primitives.long_id.SenderIdIterationNumber;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.SenderIdVertexIdData;
import org.apache.giraph.utils.VertexIdData;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An abstract structure for caching data indexed by vertex id,
 * to be sent to workers in bulk. Not thread-safe.
 *
 * @param <I> Vertex id
 * @param <T> Data
 * @param <B> Specialization of {@link VertexIdData} for T
 */
@NotThreadSafe
@SuppressWarnings("unchecked")
public abstract class SendSenderIdVertexIdDataCache<I extends WritableComparable, T,
        B extends SenderIdVertexIdData< I, T>> extends SendExtendedDataCache<B> {

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(SendVertexIdDataCache.class);

    /**
     * Constructor.
     *
     * @param conf Giraph configuration
     * @param serviceWorker Service worker
     * @param maxRequestSize Maximum request size (in bytes)
     * @param additionalRequestSize Additional request size (expressed as a
     *                              ratio of the average request size)
     */
    public SendSenderIdVertexIdDataCache(ImmutableClassesGiraphConfiguration conf,
                                 CentralizedServiceWorker<?, ?, ?> serviceWorker,
                                 int maxRequestSize,
                                 float additionalRequestSize) {
        super(conf, serviceWorker, maxRequestSize, additionalRequestSize);
    }

    /**
     * Create a new {@link VertexIdData} specialized for the use case.
     *
     * @return A new instance of {@link VertexIdData}
     */
    public abstract B createVertexIdData();

    /**
     * Add data to the cache.
     *
     * @param workerInfo the remote worker destination
     * @param partitionId the remote Partition this message belongs to
     * @param destVertexId vertex id that is ultimate destination
     * @param data Data to send to remote worker
     * @return Size of messages for the worker.
     */
    public int addData(WorkerInfo workerInfo,
                       int partitionId, SenderIdIterationNumber senderId,  I destVertexId, T data) {
        LOG.debug("Vicky --> addData");
        // Get the data collection
        SenderIdVertexIdData< I, T> partitionData =
                getPartitionData(workerInfo, partitionId);
        int originalSize = partitionData.getSize();
        LOG.debug("Vicky --> Add message to  datacache");
        partitionData.add(senderId, destVertexId, data);
        // Update the size of cached, outgoing data per worker
        return incrDataSize(workerInfo.getTaskId(),
                partitionData.getSize() - originalSize);
    }

    /**
     * This method tries to get a partition data from the data cache.
     * If null, it will create one.
     *
     * @param workerInfo The remote worker destination
     * @param partitionId The remote Partition this message belongs to
     * @return The partition data in data cache
     */
    private SenderIdVertexIdData<I, T> getPartitionData(WorkerInfo workerInfo,
                                                int partitionId) {
        LOG.debug("Vicky --> getPartitionData");
        // Get the data collection
        B partitionData = getData(partitionId);
        if (partitionData == null) {
            partitionData = createVertexIdData();
            partitionData.setConf(getConf());
            partitionData.initialize(getInitialBufferSize(workerInfo.getTaskId())); //Create extended data output
            LOG.debug("Vicky --> set message data for vertex and partition in datacache");
            setData(partitionId, partitionData);
        }

        return partitionData;
    }
}