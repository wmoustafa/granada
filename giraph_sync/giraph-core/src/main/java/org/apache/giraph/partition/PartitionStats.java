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
package org.apache.giraph.partition;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Used to keep track of statistics of every {@link Partition}. Contains no
 * actual partition data, only the statistics.
 */
public class PartitionStats implements Writable {
  /** Id of partition to keep stats for */
  private int partitionId = -1;
  /** Vertices in this partition */
  private long vertexCount = 0;
  /** Finished vertices in this partition */
  private long finishedVertexCount = 0;
  /** Edges in this partition */
  private long edgeCount = 0;
  /** Messages sent from this partition */
  private long messagesSentCount = 0;
  /** Message byetes sent from this partition */
  private long messageBytesSentCount = 0;

  /*Vicky */
  private long compute_invocations = 0;
  private long addMessageTime = 0;
  private long getMessageTime = 0;
  private long orderVerticesTime = 0;
  private long addMessageCounter = 0;
  private long getMessageCounter = 0;

  public PartitionStats(int partitionId,
                        long vertexCount,
                        long finishedVertexCount,
                        long edgeCount,
                        long messagesSentCount,
                        long messageBytesSentCount,
                        long compute_invocations,
                        long addMessageTime,
                        long getMessageTime,
                        long orderVerticesTime,
                        long addMessageCounter,
                        long getMessageCounter) {
    this.partitionId = partitionId;
    this.vertexCount = vertexCount;
    this.finishedVertexCount = finishedVertexCount;
    this.edgeCount = edgeCount;
    this.messagesSentCount = messagesSentCount;
    this.messageBytesSentCount = messageBytesSentCount;
    this.compute_invocations = compute_invocations;
    this.addMessageTime = addMessageTime;
    this.getMessageTime = getMessageTime;
    this.orderVerticesTime = orderVerticesTime;
    this.addMessageCounter = addMessageCounter;
    this.getMessageCounter = getMessageCounter;
  }

  /**
   * Default constructor for reflection.
   */
  public PartitionStats() { }

  /**
   * Constructor with the initial stats.
   *
   * @param partitionId Partition count.
   * @param vertexCount Vertex count.
   * @param finishedVertexCount Finished vertex count.
   * @param edgeCount Edge count.
   * @param messagesSentCount Number of messages sent
   * @param messageBytesSentCount Number of message bytes sent
   */
  public PartitionStats(int partitionId,
                        long vertexCount,
                        long finishedVertexCount,
                        long edgeCount,
                        long messagesSentCount,
                        long messageBytesSentCount) {
    this.partitionId = partitionId;
    this.vertexCount = vertexCount;
    this.finishedVertexCount = finishedVertexCount;
    this.edgeCount = edgeCount;
    this.messagesSentCount = messagesSentCount;
    this.messageBytesSentCount = messageBytesSentCount;
  }

  /**
   * Set the partition id.
   *
   * @param partitionId New partition id.
   */
  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }


  /**
   * Get partition id.

   *
   * @return Partition id.
   */
  public int getPartitionId() {
    return partitionId;
  }

  /**
   * Increment the vertex count by one.
   */
  public void incrVertexCount() {
    ++vertexCount;
  }

  /**
   * Get the vertex count.
   *
   * @return Vertex count.
   */
  public long getVertexCount() {
    return vertexCount;
  }

  /**
   * Increment the finished vertex count by one.
   */
  public void incrFinishedVertexCount() {
    ++finishedVertexCount;
  }

  /**
   * Get the finished vertex count.
   *
   * @return Finished vertex count.
   */
  public long getFinishedVertexCount() {
    return finishedVertexCount;
  }

  /**
   * Add edges to the edge count.
   *
   * @param edgeCount Number of edges to add.
   */
  public void addEdgeCount(long edgeCount) {
    this.edgeCount += edgeCount;
  }

  /**
   * Get the edge count.
   *
   * @return Edge count.
   */
  public long getEdgeCount() {
    return edgeCount;
  }

  /**
   * Add messages to the messages sent count.
   *
   * @param messagesSentCount Number of messages to add.
   */
  public void addMessagesSentCount(long messagesSentCount) {
    this.messagesSentCount += messagesSentCount;
  }

  /**
   * Get the messages sent count.
   *
   * @return Messages sent count.
   */
  public long getMessagesSentCount() {
    return messagesSentCount;
  }

  /**
   * Add message bytes to messageBytesSentCount.
   *
   * @param messageBytesSentCount Number of message bytes to add.
   */
  public void addMessageBytesSentCount(long messageBytesSentCount) {
    this.messageBytesSentCount += messageBytesSentCount;
  }

  /**
   * Get the message bytes sent count.
   *
   * @return Message bytes sent count.
   */
  public long getMessageBytesSentCount() {
    return messageBytesSentCount;
  }

  public long getComputeInvocations() {
    return compute_invocations;
  }

  public void addComputeInvocations(long i) {
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

  public void addOrderVertices(long time) {
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


  @Override
  public void readFields(DataInput input) throws IOException {
    partitionId = input.readInt();
    vertexCount = input.readLong();
    finishedVertexCount = input.readLong();
    edgeCount = input.readLong();
    messagesSentCount = input.readLong();
    messageBytesSentCount = input.readLong();
    compute_invocations = input.readLong();
    addMessageTime = input.readLong();
    getMessageTime = input.readLong();
    orderVerticesTime = input.readLong();
    addMessageCounter = input.readLong();
    getMessageCounter = input.readLong();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(partitionId);
    output.writeLong(vertexCount);
    output.writeLong(finishedVertexCount);
    output.writeLong(edgeCount);
    output.writeLong(messagesSentCount);
    output.writeLong(messageBytesSentCount);
    output.writeLong(compute_invocations);
    output.writeLong(addMessageTime);
    output.writeLong(getMessageTime);
    output.writeLong(orderVerticesTime);
    output.writeLong(addMessageCounter);
    output.writeLong(getMessageCounter);

  }

  @Override
  public String toString() {
    return "(id=" + partitionId + ",vtx=" + vertexCount + ",finVtx=" +
            finishedVertexCount + ",edges=" + edgeCount + ",msgsSent=" +
            messagesSentCount + ",msgBytesSent=" +  messageBytesSentCount +
            ",computeInvocations="  + compute_invocations +
            ", addMessageTime=" + addMessageTime + ", getMessageTime=" + getMessageTime +
            ", orderVerticesTime=" + orderVerticesTime +
            ", addMessageCounter = " + addMessageCounter +
            ", getMessageCounter = " + getMessageCounter  + ")";
  }
}
