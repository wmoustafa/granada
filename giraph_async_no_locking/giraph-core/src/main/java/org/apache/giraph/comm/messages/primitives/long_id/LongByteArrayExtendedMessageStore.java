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

package org.apache.giraph.comm.messages.primitives.long_id;

/**
 * Created by Vicky Papavasileiou on 7/31/15.
 */

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.messages.MessagesIterable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.*;
import org.apache.giraph.utils.io.DataInputOutput;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;


/**
 * Special message store to be used when ids are LongWritable and no combiner
 * is used.
 * Uses fastutil primitive maps in order to decrease number of objects and
 * get better performance.
 *
 * @param <M> Message type
 */
public class LongByteArrayExtendedMessageStore<M extends Writable>
        extends LongAbstractExtendedMessageStore<M, DataInputOutput> {

    // Vicky
    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(LongByteArrayExtendedMessageStore.class);

    private long addMessageTimer = 0;
    private long getMessageTimer = 0;

    /**
     * Constructor
     *
     * @param messageValueFactory Factory for creating message values
     * @param service             Service worker
     * @param config              Hadoop configuration
     */
    public LongByteArrayExtendedMessageStore(
            MessageValueFactory<M> messageValueFactory,
            CentralizedServiceWorker<LongWritable, Writable, Writable> service,
            ImmutableClassesGiraphConfiguration<LongWritable,
                    Writable, Writable> config) {
        super(messageValueFactory, service, config);
    }

    @Override
    public boolean isPointerListEncoding() {
        return false;
    }

    /**
     * Get the DataInputOutput for a vertex id, creating if necessary.
     *
     * @param partitionMap Partition map to look in
     * @param vertexId Id of the vertex
     * @return DataInputOutput for this vertex id (created if necessary)
     */
    private DataInputOutput getDataInputOutput(
            Long2ObjectOpenHashMap<DataInputOutput> partitionMap, long vertexId) {
        DataInputOutput dataInputOutput = partitionMap.get(vertexId);
        if (dataInputOutput == null) {
            dataInputOutput = config.createMessagesInputOutput();
            partitionMap.put(vertexId, dataInputOutput);
        }
        return dataInputOutput;
    }

    public long getAddMessageTimer() { return this.addMessageTimer;}

    public long getGetMessageTimer() { return this.getMessageTimer; }

    @Override
    public void addPartitionMessages(int partitionId,
                                     VertexIdMessages<LongWritable, M> messages) throws IOException {
        throw new UnsupportedOperationException();
    }


    public void addPartitionMessages(int partitionId, SenderIdVertexIdMessages<LongWritable, M> messages) throws IOException {
        throw new UnsupportedOperationException();
    }


    public void addExtendedPartitionMessages(int partitionId,
                                             SenderIdVertexIdMessages<LongWritable, M> messages) throws IOException {


        long start = System.currentTimeMillis();

        //LOG.debug(" addExtendedPartitionMessages: partition " + partitionId);
        Map<SenderIdIterationNumber,Long2ObjectOpenHashMap<DataInputOutput>> outerMap =  map.get(partitionId);


        SenderIdVertexIdMessageBytesIterator<LongWritable, M>
                vertexIdMessageBytesIterator = messages.getSenderIdVertexIdMessageBytesIterator();

        synchronized (outerMap) {

        //TODO Overwrite messages from same sender if not already processed
        if (vertexIdMessageBytesIterator != null) {

            //LOG.debug("Vicky --> addExtendedPartitionMessages message bytes iterator");
            while (vertexIdMessageBytesIterator.hasNext()) {

                vertexIdMessageBytesIterator.next();

                Long2ObjectOpenHashMap<DataInputOutput> vertexMap = null;
                SenderIdIterationNumber sender_info = vertexIdMessageBytesIterator.getCurrentSenderId();

                if(outerMap.get(sender_info) == null) {
                    if (outerMap.get(sender_info) == null) {
                        //LOG.debug("Vicky --> addExtendedPartitionMessages create new vertex map");
                        vertexMap = new Long2ObjectOpenHashMap<>();
                        outerMap.put(sender_info, vertexMap);
                    }
                }else {
                    //LOG.debug("Vicky --> addExtendedPartitionMessages map already exists");
                    vertexMap = outerMap.get(sender_info);
                }

                assert(vertexMap!=null);
                DataInputOutput dataInputOutput = config.createMessagesInputOutput();
                vertexMap.put(vertexIdMessageBytesIterator.getCurrentVertexId().get(), dataInputOutput);

                vertexIdMessageBytesIterator.writeCurrentMessageBytes(
                        dataInputOutput.getDataOutput());
            }
        } else {
            SenderIdVertexIdMessageIterator<LongWritable, M>
                    iterator = messages.getSenderIdVertexIdMessageIterator();
            while (iterator.hasNext()) {
                iterator.next();
                Long2ObjectOpenHashMap<DataInputOutput> vertexMap = null;
                SenderIdIterationNumber sender_info = iterator.getCurrentSenderId();
                //LOG.debug("Vicky --> addPartitionMsg , sender = " + sender_info);

                if (outerMap.get(sender_info) == null) {
                    //LOG.debug("Vicky --> addExtendedPartitionMessages create new vertex map");
                    vertexMap = new Long2ObjectOpenHashMap<>();
                    outerMap.put(sender_info, vertexMap);
                    //TODO why do i call this here??
                    //sender_info.initialize_map();
                    //LOG.debug("Vicky --> sender info " + sender_info.toString());
                }
                else {
                    //LOG.debug("Vicky --> addExtendedPartitionMessages map already exists");
                    vertexMap = outerMap.get(sender_info);
                }

                LOG.info("Vicky --> addExtendedPartitionMessages Add message for: pID = " + partitionId +
                        ", vId = " + iterator.getCurrentVertexId().get() + ", data =" + iterator.getCurrentData());


                //TODO if new message value smaller than old message, replace else skip
                if(vertexMap.get(iterator.getCurrentVertexId().get()) != null)
                    LOG.info("Vicky --> old message = " + vertexMap.get(iterator.getCurrentVertexId()).create().readDouble() +
                    " new message = " + ((DoubleWritable)iterator.getCurrentData()).get());

                if(vertexMap.get(iterator.getCurrentVertexId().get()) != null &&
                        vertexMap.get(iterator.getCurrentVertexId().get()).create().readDouble() >
                        ((DoubleWritable)iterator.getCurrentData()).get()) {


                    for (SenderIdIterationNumber old : outerMap.keySet()) {
                        //LOG.info("Vicky --> old sender: " + old );
                        if (old.equals(sender_info)) {

                            //LOG.info(" Vicky --> new sender : " + sender_info);
                            //LOG.info("Vicky --> mark unread by neighbor " + iterator.getCurrentVertexId());
                            synchronized (sender_info.getMap()) {
                                old.markUnreadByNeighbor(iterator.getCurrentVertexId().get());
                            }
                        }
                    }
                    DataInputOutput dataInputOutput = config.createMessagesInputOutput();
                    vertexMap.put(iterator.getCurrentVertexId().get(), dataInputOutput);
                    VerboseByteStructMessageWrite.verboseWriteCurrentExtendedMessage(iterator,
                            dataInputOutput.getDataOutput());
                }
                else if(vertexMap.get(iterator.getCurrentVertexId().get()) == null)
                {
                    //LOG.info("Vicky --> addExtendedPartitionMessages Add message for: pID = " + partitionId +
                     //       ", vId = " + iterator.getCurrentVertexId().get() + ", data =" + iterator.getCurrentData());

                    for (SenderIdIterationNumber old : outerMap.keySet()) {
                        //LOG.info("Vicky --> old sender: " + old );
                        if (old.equals(sender_info)) {

                            //LOG.info(" Vicky --> new sender : " + sender_info);
                            //LOG.info("Vicky --> mark unread by neighbor " + iterator.getCurrentVertexId());
                            synchronized (sender_info.getMap()) {
                                old.markUnreadByNeighbor(iterator.getCurrentVertexId().get());
                            }
                        }
                    }

                    DataInputOutput dataInputOutput = config.createMessagesInputOutput();
                    vertexMap.put(iterator.getCurrentVertexId().get(), dataInputOutput);
                    VerboseByteStructMessageWrite.verboseWriteCurrentExtendedMessage(iterator,
                            dataInputOutput.getDataOutput());

                }


            }
        }

        }
        long end = System.currentTimeMillis();
        addMessageTimer+= (end-start);
        //LOG.info("Vicky --> addExtendedPartitionMessages add one message time " + (end-start));
    }

    @Override
    public void finalizeStore() {
        LOG.debug("Vicky --> LongByteArrayMessageStore.finalizeStore (no op)");
    }

    @Override
    public Iterable<M> getVertexMessages(LongWritable vertexId)
            throws IOException {

        //LOG.info("Vicky --> getExtendedVertexMessages for vertexId=[" + vertexId + "]");

        long start = System.currentTimeMillis();
        DataInputOutput dataInputOutput = config.createMessagesInputOutput();

        Map<SenderIdIterationNumber,Long2ObjectOpenHashMap<DataInputOutput>> extended_sender_map =
                getPartitionMap(vertexId);
        for(Map.Entry<SenderIdIterationNumber,Long2ObjectOpenHashMap<DataInputOutput>> sender:
                extended_sender_map.entrySet()) {
            Long2ObjectOpenHashMap<DataInputOutput> vertex_map = sender.getValue();
            if(vertex_map.containsKey(vertexId.get()))
            {
               // LOG.info("Vicky --> get messages from sender: " + sender.getKey().toString());
                //TODO Check if message has already been read
                if (sender.getKey().isMessageReadByNeighbor(vertexId.get()) == false) {

                    //Need to create a collection that will hold all messages for a specific destination vertex
                    //LOG.info("Vicky --> read double: " + vertex_map.get(vertexId.get()).create().readDouble());
                    dataInputOutput.getDataOutput().writeDouble(vertex_map.get(vertexId.get()).create().readDouble());
                    synchronized (sender.getKey().getMap()) {
                        sender.getKey().setMessageReadByNeighbor(vertexId.get());
                    }
                }
            }
        }
        long end = System.currentTimeMillis();
        getMessageTimer+= (end-start);
        //LOG.info("Vicky --> getVertexMessages get messages for one vertex time " + (end-start));
        return new MessagesIterable<M>(dataInputOutput, messageValueFactory);

    }


    public boolean hasUnreadMessages(int pId) {
        Map<SenderIdIterationNumber, Long2ObjectOpenHashMap<DataInputOutput>> partition_map = getExtendedPartitionMap(pId);


        for (SenderIdIterationNumber sender : partition_map.keySet()) {
            for (Long vId : (Set<Long>)sender.getMap().keySet()) {
                if (sender.getMap().get(vId).equals(false))
                    return true;
            }
        }
        return false;
    }


    //Vicky
    public void printExtendendMap() throws Exception {
        int size = 0;
        StringBuilder sb = new StringBuilder();
        for (int i : map.keySet()) {
            sb.append("pID: [" + i + "\n");
            synchronized (map.get(i).keySet()) {
                for (SenderIdIterationNumber sender : map.get(i).keySet()) {
                    sb.append("[sender: " + sender);
                    for (Long vId : map.get(i).get(sender).keySet()) {
                        sb.append("{vId: " + vId);
                        DataInputOutput dataInputOutput = map.get(i).get(sender).get(vId);
                        if (dataInputOutput != null) {
                            Iterable<M> messages = new MessagesIterable<M>(dataInputOutput, messageValueFactory);
                            sb.append(" , messages =  ");
                            for (M message : messages) {
                                size++;
                                sb.append(message + ", ");
                            }
                            sb.append("},");
                        }
                    }
                    sb.append("]\n");
                }
            }
            sb.append("\n");
        }
        LOG.info("\n printExtendedMap of store size =  " + size +"\n" + sb.toString());
    }

    @Override
    public void writePartition(DataOutput out, int partitionId)
            throws IOException {
       throw new UnsupportedOperationException();
    }

    @Override
    public void readFieldsForPartition(DataInput in,
                                       int partitionId) throws IOException {
        throw new UnsupportedOperationException();
    }
}
