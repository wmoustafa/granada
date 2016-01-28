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

/**
 * Created by Vicky Papavasileiou on 7/30/15.
 */
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Stores pairs of vertex id and generic data in a single byte array
 *
 * @param <I> Vertex id
 * @param <T> Data
 */
public abstract class ByteArraySenderIdVertexIdData< I extends WritableComparable,
        T> extends AbstractSenderIdVertexIdData< I, T> {


    // Vicky
    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(ByteArrayVertexIdData.class);


    /**
     * Get the underlying byte-array.
     *
     * @return The underlying byte-array
     */
    public byte[] getByteArray() {
        return extendedDataOutput.getByteArray();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        //LOG.debug("Vicky --> write to extended data output");
        WritableUtils.writeExtendedDataOutput(extendedDataOutput, dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        extendedDataOutput =
                WritableUtils.readExtendedDataOutput(dataInput, getConf());
    }
}
