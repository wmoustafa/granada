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
import java.util.Iterator;

/**
 * This iterator is designed to deserialize a byte array on the fly to
 * provide new copies of writable objects when desired.  It does not reuse
 * objects, and instead creates a new one for every next().
 *
 * @param <T> Type that extends Writable that will be iterated
 */
public abstract class ByteStructIterator<T extends Writable> implements
    Iterator<T> {

  // Vicky
  /** Class logger */
  private static final Logger LOG =
          Logger.getLogger(ByteStructIterator.class);

  private int start_pos = Integer.MAX_VALUE;
  private int end_pos = Integer.MAX_VALUE;


  /** Data input */
  protected final ExtendedDataInput extendedDataInput;

  /**
   * Wrap ExtendedDataInput in ByteArrayIterator
   *
   * @param extendedDataInput ExtendedDataInput
   */
  public ByteStructIterator(ExtendedDataInput extendedDataInput) {
    this.extendedDataInput = extendedDataInput;
  }

  /**
   * Vicky Wrap ExtendedDataInput in ByteArrayIterator
   *
   * @param extendedDataInput ExtendedDataInput
   */
  public ByteStructIterator(ExtendedDataInput extendedDataInput, int start_pos, int end_pos) {
    this.extendedDataInput = extendedDataInput;
    this.start_pos = start_pos;
    this.end_pos = end_pos;
  }

  @Override
  public boolean hasNext() {

    //LOG.info("Vicky --> hasNext current_ pos=" + extendedDataInput.getPos() + ", end_pos = " + end_pos);
    //LOG.info("Vicky --> hasNext " + ((!extendedDataInput.endOfInput()) &&
    //                ((((UnsafeByteArrayInputStream)extendedDataInput).getPos()) < end_pos)));
    return ((!extendedDataInput.endOfInput()) && (((UnsafeByteArrayInputStream)extendedDataInput).getPos()) < end_pos);
  }

  @Override
  public T next() {
    T writable = createWritable();
    try {
      writable.readFields(extendedDataInput);
    } catch (IOException e) {
      throw new IllegalStateException("next: readFields got IOException", e);
    }
    return writable;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove: Not supported");
  }

  /**
   * Must be able to create the writable object
   *
   * @return New writable
   */
  protected abstract T createWritable();
}
