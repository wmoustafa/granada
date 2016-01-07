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
package org.apache.giraph.types.ops;

import org.apache.giraph.types.ops.collections.BasicArrayList;


/**
 * Type operations, allowing working generically with types,
 * but still having efficient code.
 *
 * Using any of the provided operations should lead to no boxing/unboxing.
 *
 * Useful generic wrappers to fastutil libraries are provided,
 * so that you can look at them generically.
 *
 * @param <T> Type
 */
public interface PrimitiveTypeOps<T> extends TypeOps<T> {
  // primitive collections
  /**
   * Create BasicArrayList of type T.
   * @return BasicArrayList
   */
  BasicArrayList<T> createArrayList();

  /**
   * Create BasicArrayList of type T, given capacity.
   * @param capacity Capacity
   * @return BasicArrayList
   */
  BasicArrayList<T> createArrayList(int capacity);
}
