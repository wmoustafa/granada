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

import org.apache.giraph.utils.PairList;
import org.apache.giraph.utils.SenderIdVertexIdData;
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
public abstract class SendWorkerExtendedDataRequest<I extends WritableComparable, T,
        B extends SenderIdVertexIdData<I, T>>
        extends WritableRequest implements WorkerRequest {


    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(SendWorkerExtendedDataRequest.class);

    /**
     * All data for a group of vertices, organized by partition, which
     * are owned by a single (destination) worker. This data is all
     * destined for this worker.
     * */
    protected PairList<Integer, B> partitionVertexData;


    /**
     * Constructor used for reflection only
     */
    public SendWorkerExtendedDataRequest() { }

    /**
     * Constructor used to send request.
     *
     * @param partVertData Map of remote partitions => VertexIdData
     */
    public SendWorkerExtendedDataRequest(
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
    public abstract B createSenderIdVertexIdData();

    @Override
    public void readFieldsRequest(DataInput input) throws IOException {
        LOG.debug("Vicky --> Read request");
        int numPartitions = input.readInt();
        partitionVertexData = new PairList<Integer, B>();
        partitionVertexData.initialize(numPartitions);
        while (numPartitions-- > 0) {
          final int partitionId = input.readInt();
          B vertexIdData = createSenderIdVertexIdData();
          vertexIdData.setConf(getConf());
          vertexIdData.readFields(input);
          partitionVertexData.add(partitionId, vertexIdData);
          LOG.info(vertexIdData);
        }
    }


    @Override
    public void writeRequest(DataOutput output) throws IOException {
        LOG.debug("Vicky --> writeRequest ");
        output.writeInt(partitionVertexData.getSize());
        PairList<Integer, B>.Iterator
                iterator = partitionVertexData.getIterator();
        while (iterator.hasNext()) {
            iterator.next();
            LOG.debug("Vicky --> " + iterator.getCurrentFirst() + " , " + iterator.getCurrentSecond());
            output.writeInt(iterator.getCurrentFirst());
            iterator.getCurrentSecond().write(output);
        }
    }



    @Override
    public int getSerializedSize() {
        int size = super.getSerializedSize() + 4;
        PairList<Integer, B>.Iterator iterator = partitionVertexData.getIterator();
        while (iterator.hasNext()) {
            iterator.next();
            size += 4 + iterator.getCurrentSecond().getSerializedSize();
        }
        return size;
    }
}


