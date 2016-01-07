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

import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * The objects provided by this iterator have lifetimes only until next() is
 * called.  In that sense, the object provided is only a representative object.
 *
 * @param <T> Type that extends Writable that will be iterated
 */
public abstract class RepresentativeByteStructIterator<T extends
    Writable> extends ByteStructIterator<T> {

  // Vicky
  /** Class logger */
  private static final Logger LOG =
          Logger.getLogger(RepresentativeByteStructIterator.class);

  private int start_pos = -1;
  private int end_pos = -1;

  /** Representative writable */
  private final T representativeWritable = createWritable();

  /**
   * Wrap ExtendedDataInput in ByteArrayIterator
   *
   * @param extendedDataInput ExtendedDataInput
   */
  public RepresentativeByteStructIterator(ExtendedDataInput extendedDataInput) {
    super(extendedDataInput);

  }

  /**
   * Vicky Wrap ExtendedDataInput in ByteArrayIterator
   *
   * @param extendedDataInput ExtendedDataInput
   */
  public RepresentativeByteStructIterator(ExtendedDataInput extendedDataInput, int start, int end) {
    super(extendedDataInput, start,end);
    start_pos = start;
    end_pos = end;
  }


  @Override
  public T next() {
    try {
      //LOG.info("Vicky --> Skipping " + start_pos + " bytes, current pos =  " + ((UnsafeByteArrayInputStream) extendedDataInput).getPos());
      //TODO I think i don't need this as I don't need to skip bytes
      //((DoubleWritable)representativeWritable).set(((UnsafeByteArrayInputStream) extendedDataInput).readDoubleWithSkip(0));
      //LOG.info("Vicky --> RepresentativeWritable next returns" + ((DoubleWritable)representativeWritable).get());
      representativeWritable.readFields(extendedDataInput);
    } catch (IOException e) {
      LOG.info("Vicky --> Skipping " + start_pos + " bytes, current pos =  " + ((UnsafeByteArrayInputStream) extendedDataInput).getPos());
      throw new IllegalStateException("next: readFields got IOException", e);

    }
    return representativeWritable;
  }
}
