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

import java.util.Iterator;

/**
 * The objects provided by the iterators generated from this object have
 * lifetimes only until next() is called.  In that sense, the object
 * provided is only a representative object.
 *
 * @param <T> Type that extends Writable that will be iterated
 */
public abstract class RepresentativeByteStructIterable<T extends Writable>
    extends ByteStructIterable<T> {

  private int start_pos;
  private int end_pos;

  /**
   * Default Constructor
   *
   * @param dataInputFactory Factory for data inputs
   */
  public RepresentativeByteStructIterable(
      Factory<? extends ExtendedDataInput> dataInputFactory) {
    super(dataInputFactory);

  }

  /**
   * Constructor
   *
   * @param dataInputFactory Factory for data inputs
   */
  public RepresentativeByteStructIterable(
          Factory<? extends ExtendedDataInput> dataInputFactory, int start_pos, int end_pos) {
    super(dataInputFactory);
    this.start_pos = start_pos;
    this.end_pos = end_pos;
  }


  @Override
  public Iterator<T> iterator() {
    return new RepresentativeByteStructIterator<T>(dataInputFactory.create(),this.start_pos,this.end_pos) {
      @Override
      protected T createWritable() {
        return RepresentativeByteStructIterable.this.createWritable();
      }
    };
  }
}
