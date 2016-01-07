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
package org.apache.giraph.block_app.framework.api.giraph;

import org.apache.giraph.block_app.framework.api.BlockWorkerContextReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerContextSendApi;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.Writable;

/**
 * Giraph implementation of BlockWorkerContextReceiveApi and
 * BlockWorkerContextSendApi, passing all calls to WorkerContext.
 *
 * @param <WM> Worker message type
 */
final class BlockWorkerContextApiWrapper<WM extends Writable>
    implements BlockWorkerContextReceiveApi, BlockWorkerContextSendApi<WM> {
  private final WorkerContext workerContext;

  public BlockWorkerContextApiWrapper(WorkerContext workerContext) {
    this.workerContext = workerContext;
  }

  @Override
  public ImmutableClassesGiraphConfiguration<?, ?, ?> getConf() {
    return workerContext.getConf();
  }

  @Override
  public int getWorkerCount() {
    return workerContext.getWorkerCount();
  }

  @Override
  public int getMyWorkerIndex() {
    return workerContext.getMyWorkerIndex();
  }

  @Override
  public <A extends Writable> A getAggregatedValue(String name) {
    return workerContext.getAggregatedValue(name);
  }

  @Override
  public <A extends Writable> void aggregate(String name, A value) {
    workerContext.aggregate(name, value);
  }

  @Override
  public void sendMessageToWorker(WM message, int workerIndex) {
    workerContext.sendMessageToWorker(message, workerIndex);
  }

  @Override
  public <B extends Writable> B getBroadcast(String name) {
    return workerContext.getBroadcast(name);
  }

  @Override
  public long getTotalNumEdges() {
    return workerContext.getTotalNumEdges();
  }

  @Override
  public long getTotalNumVertices() {
    return workerContext.getTotalNumVertices();
  }
}
