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

package org.apache.giraph.graph;

import org.apache.giraph.bsp.checkpoints.CheckpointStatus;
import org.apache.giraph.partition.PartitionStats;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Aggregated stats by the master.
 */
public class GlobalStats implements Writable {

  private static final Logger LOG  = Logger.getLogger(GlobalStats.class);

  /** All vertices in the application */
  private long vertexCount = 0;
  /** All finished vertices in the last superstep */
  private long finishedVertexCount = 0;
  /** All edges in the last superstep */
  private long edgeCount = 0;
  /** All messages sent in the last superstep */
  private long messageCount = 0;
  /** All message bytes sent in the last superstep */
  private long messageBytesCount = 0;
  /** Whether the computation should be halted */
  private boolean haltComputation = false;

  /*Vicky */
  private long compute_invocations = 0;
  private long addMessageTime = 0;
  private long getMessageTime = 0;
  private long orderVerticesTime = 0;
  private long addMessageCounter = 0;
  private long getMessageCounter = 0;


  /**
   * Master's decision on whether we should checkpoint and
   * what to do next.
   */
  private CheckpointStatus checkpointStatus =
          CheckpointStatus.NONE;

  /**
   * Add the stats of a partition to the global stats.
   *
   * @param partitionStats Partition stats to be added.
   */
  public void addPartitionStats(PartitionStats partitionStats) {
    this.vertexCount += partitionStats.getVertexCount();
    this.finishedVertexCount += partitionStats.getFinishedVertexCount();
    this.edgeCount += partitionStats.getEdgeCount();
    /*this.compute_invocations += partitionStats.getComputeInvocations();
    this.addMessageTime += partitionStats.getAddMessageTime();
    this.getMessageTime += partitionStats.getGetMessageTime();
    this.orderVerticesTime += partitionStats.getOrderVerticesTime();*/
  }

  public long getVertexCount() {
    return vertexCount;
  }

  public long getFinishedVertexCount() {
    return finishedVertexCount;
  }

  public long getEdgeCount() {
    return edgeCount;
  }

  public long getMessageCount() {
    return messageCount;
  }

  public long getMessageBytesCount() {
    return messageBytesCount;
  }

  public boolean getHaltComputation() {
    return haltComputation;
  }

  public long getComputeInvocations() {
    return compute_invocations;
  }

  public void addComputeInvocations(long i){
    this.compute_invocations += i;
  }

  public long getAddMessageTime() {
    return addMessageTime;
  }

  public void addAddMessageTime(long time){
    this.addMessageTime += time;
  }

  public long getGetMessageTime() {
    return getMessageTime;
  }

  public void addGetMessageTime(long time){
    this.getMessageTime += time;
  }

  public long getOrderVerticesTime() {
    return orderVerticesTime;
  }

  public void addOrderedVerticesTime(long time){
    this.orderVerticesTime += time;
  }

  public long getAddMessageCounter() {
    return addMessageCounter;
  }

  public void addAddMessageCounter(long i){
    this.addMessageCounter += i;
  }

  public long getGetMessageCounter() {
    return getMessageCounter;
  }

  public void addGetMessageCounter(long i){
    this.getMessageCounter += i;
  }


  public void setHaltComputation(boolean value) {
    haltComputation = value;
  }

  public CheckpointStatus getCheckpointStatus() {
    return checkpointStatus;
  }

  public void setCheckpointStatus(CheckpointStatus checkpointStatus) {
    this.checkpointStatus = checkpointStatus;
  }

  /**
   * Add messages to the global stats.
   *
   * @param messageCount Number of messages to be added.
   */
  public void addMessageCount(long messageCount) {
    this.messageCount += messageCount;
  }

  /**
   * Add messages to the global stats.
   *
   * @param msgBytesCount Number of message bytes to be added.
   */
  public void addMessageBytesCount(long msgBytesCount) {
    this.messageBytesCount += msgBytesCount;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    vertexCount = input.readLong();
    finishedVertexCount = input.readLong();
    edgeCount = input.readLong();
    messageCount = input.readLong();
    messageBytesCount = input.readLong();
    haltComputation = input.readBoolean();
    if (input.readBoolean()) {
      checkpointStatus = CheckpointStatus.values()[input.readInt()];
    } else {
      checkpointStatus = null;
    }
    compute_invocations = input.readLong();
    LOG.info("GlobalStats read compute invocations = " + this.compute_invocations);
    addMessageTime = input.readLong();
    getMessageTime = input.readLong();
    orderVerticesTime = input.readLong();
    addMessageCounter = input.readLong();
    getMessageCounter = input.readLong();

  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeLong(vertexCount);
    output.writeLong(finishedVertexCount);
    output.writeLong(edgeCount);
    output.writeLong(messageCount);
    output.writeLong(messageBytesCount);
    output.writeBoolean(haltComputation);
    output.writeBoolean(checkpointStatus != null);
    if (checkpointStatus != null) {
      output.writeInt(checkpointStatus.ordinal());
    }
    output.writeLong(compute_invocations);
    output.writeLong(addMessageTime);
    output.writeLong(getMessageTime);
    output.writeLong(orderVerticesTime);
    output.writeLong(addMessageCounter);
    output.writeLong(getMessageCounter);


  }

  @Override
  public String toString() {
    return "(vtx=" + vertexCount + ",finVtx=" +
            finishedVertexCount + ",edges=" + edgeCount + ",msgCount=" +
            messageCount + ",msgBytesCount=" +
            messageBytesCount + ",haltComputation=" + haltComputation +
            ", checkpointStatus=" + checkpointStatus +
            ",computeInvocations="  + compute_invocations +
            ", addMessageTime=" + addMessageTime + ", getMessageTime=" + getMessageTime +
            ", orderVerticesTime=" + orderVerticesTime +
            ", addMessageCounter = " + addMessageCounter +
            ", getMessageCounter = " + getMessageCounter  + ")";
  }
}
