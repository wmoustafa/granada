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

package org.apache.giraph.utils;

import org.apache.giraph.comm.messages.primitives.long_id.SenderIdIterationNumber;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by root on 7/30/15.
 * Vicky Papavasileiou
 */
public class ByteArraySenderIdVertexIdMessages <I extends WritableComparable,
        M extends Writable> extends ByteArraySenderIdVertexIdData<I, M>
        implements SenderIdVertexIdMessages<I, M> {

    // Vicky
    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(ByteArrayVertexIdMessages.class);

    /** Message value class */
    private final MessageValueFactory<M> messageValueFactory;
    /** Add the message size to the stream? (Depends on the message store) */
    private boolean useMessageSizeEncoding = false;

    /**
     * Constructor
     *
     * @param messageValueFactory Class for messages
     */
    public ByteArraySenderIdVertexIdMessages(
            MessageValueFactory<M> messageValueFactory) {
        this.messageValueFactory = messageValueFactory;
    }

    /**
     * Set whether message sizes should be encoded.  This should only be a
     * possibility when not combining.  When combining, all messages need to be
     * de-serialized right away, so this won't help.
     */
    private void setUseMessageSizeEncoding() {
        if (!getConf().useOutgoingMessageCombiner()) {
            useMessageSizeEncoding = getConf().useMessageSizeEncoding();
        } else {
            useMessageSizeEncoding = false;
        }
    }

    @Override
    public M createData() {
        return messageValueFactory.newInstance();
    }

    @Override
    public void writeData(ExtendedDataOutput out, M message) throws IOException {
        //LOG.debug("Vicky --> writeData: write message to ByreArray");
        //LOG.debug("Vicky --> writeData: Messages are of instance " + message.getClass().getSimpleName());
        //LOG.debug("Vicky --> writeData: msg = " + message);
        message.write(out);
    }

    @Override
    public void readData(ExtendedDataInput in, M message) throws IOException {
        message.readFields(in);
    }

    @Override
    public void initialize() {
        super.initialize();
        setUseMessageSizeEncoding();
    }

    @Override
    public void initialize(int expectedSize) {
        super.initialize(expectedSize);
        setUseMessageSizeEncoding();
    }

    @Override
    public ByteStructSenderIdVertexIdMessageIterator<I, M> getSenderIdVertexIdMessageIterator() {
        return new ByteStructSenderIdVertexIdMessageIterator<>(this);
    }

    @Override
    public void add(SenderIdIterationNumber senderId, I vertexId, M message) {
        //LOG.debug("Vicky --> ByteArrayVertexIdMessages:add ");
        if (!useMessageSizeEncoding) {
            //LOG.debug("Vicky--> Call super add");
            super.add(senderId, vertexId, message);
        } else {
            try {
                //LOG.debug("Vicky --> Write to extendedDataOutput");
                vertexId.write(extendedDataOutput);
                writeMessageWithSize(message);
            } catch (IOException e) {
                throw new IllegalStateException("add: IOException occurred");
            }
        }
    }


    /**
     * Write a size of the message and message
     *
     * @param message Message to write
     */
    private void writeMessageWithSize(M message) throws IOException {
        int pos = extendedDataOutput.getPos();
        extendedDataOutput.skipBytes(4);
        writeData(extendedDataOutput, message);
        extendedDataOutput.writeInt(
                pos, extendedDataOutput.getPos() - pos - 4);
    }

    @Override
    public ByteStructSenderIdVertexIdMessageBytesIterator< I, M>
    getSenderIdVertexIdMessageBytesIterator() {
        if (!useMessageSizeEncoding) {
            return null;
        }
        return new ByteStructSenderIdVertexIdMessageBytesIterator< I, M>(this) {
            @Override
            public void writeCurrentMessageBytes(DataOutput dataOutput) {
                try {
                    //LOG.debug("Vicky --> DataOutput instance of " + dataOutput.getClass().getSimpleName());
                    dataOutput.write(extendedDataOutput.getByteArray(),
                            messageOffset, messageBytes);
                } catch (NegativeArraySizeException e) {
                    VerboseByteStructMessageWrite.handleNegativeArraySize(vertexId);
                } catch (IOException e) {
                    throw new IllegalStateException("writeCurrentMessageBytes: Got " +
                            "IOException", e);
                }
            }
        };
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        //LOG.debug("Vicky --> write to data output");
        dataOutput.writeBoolean(useMessageSizeEncoding);
        super.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        useMessageSizeEncoding = dataInput.readBoolean();
        super.readFields(dataInput);
    }
}
