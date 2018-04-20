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

package org.apache.giraph.examples;

import com.google.common.collect.Iterables;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.counters.GiraphStats;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.GlobalStats;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
@Algorithm(
    name = "Shortest paths",
    description = "Finds all shortest paths from a selected vertex"
)
public class SimpleShortestPathsComputationCustom extends BasicComputation<
    LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
  /** The shortest paths id */
  public static final LongConfOption SOURCE_ID =
      new LongConfOption("SimpleShortestPathsVertex.sourceId", 0,
          "The shortest paths id");
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(SimpleShortestPathsComputationCustom.class);

  /**
   * Is this vertex the source id?
   *
   * @param vertex Vertex
   * @return True if the source id
   */
  private boolean isSource(Vertex<LongWritable, ?, ?> vertex) {
    return vertex.getId().get() == SOURCE_ID.get(getConf());
  }

  @Override
  public void compute(
      Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
      Iterable<DoubleWritable> messages) throws IOException {
    if (getSuperstep() == 0) {
      vertex.setValue(new DoubleWritable(Double.MAX_VALUE));
    }

    if(compensationFunctionEnabled()){
      System.out.println("Compensate function in superstep " + getSuperstep()
              + " at attempt " + getContext().getTaskAttemptID().getId());
      
	  
//	  compensateVertex(vertex);
		// for the failed worker, the initial value of all vertex is 0
		if(vertex.getValue().equals(new DoubleWritable(0))){
		  System.out.println("Compensate function vertex " + vertex.getId() + " by resetting values");
		  vertex.setValue(new DoubleWritable(Double.MAX_VALUE));
		} else {

		  System.out.println("Compensate function vertex " + vertex.getId() + " by sending message");
		  // for the success worker, send messages to their edges
		  
		  for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
			double distance = vertex.getValue().get() + edge.getValue().get();
			if (LOG.isDebugEnabled()) {
			  LOG.debug("Vertex " + vertex.getId() + " sent to " +
				  edge.getTargetVertexId() + " = " + distance);
			}

			System.out.println(getSuperstep() + " " + getMyWorkerIndex() + " " + getConf().getLocalHostname()
					+ " " + getConf().getTaskPartition() + " " +
					"Vertex " + vertex.getId() + " sent to " +
					edge.getTargetVertexId() + " = " + distance);
			sendMessage(edge.getTargetVertexId(), new DoubleWritable(distance));
		  }
		}
    }

    // check whether to kill this process or not
    if(killProcessEnabled(getConf().getSuperstepToKill())){
      System.out.println("Kill process in superstep " + getSuperstep()
              + " at attempt " + getContext().getTaskAttemptID().getId());
      System.exit(-1);
    }

    double minDist = isSource(vertex) ? 0d : Double.MAX_VALUE;
    for (DoubleWritable message : messages) {
      minDist = Math.min(minDist, message.get());
    }

    // update to check the trace
    if (LOG.isDebugEnabled()) {
      LOG.debug("Vertex " + vertex.getId() + " got minDist = " + minDist +
          " vertex value = " + vertex.getValue());
    }
    System.out.println(getSuperstep() + " " + getMyWorkerIndex() + " " + getConf().getLocalHostname()
            + " " + getConf().getTaskPartition() + " " +
            "Vertex " + vertex.getId() + " got minDist = " + minDist +
            " vertex value = " + vertex.getValue());

    if (minDist < vertex.getValue().get()) {
      vertex.setValue(new DoubleWritable(minDist));
      for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
        double distance = minDist + edge.getValue().get();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Vertex " + vertex.getId() + " sent to " +
              edge.getTargetVertexId() + " = " + distance);
        }

        System.out.println(getSuperstep() + " " + getMyWorkerIndex() + " " + getConf().getLocalHostname()
                + " " + getConf().getTaskPartition() + " " +
                "Vertex " + vertex.getId() + " sent to " +
                edge.getTargetVertexId() + " = " + distance);
        sendMessage(edge.getTargetVertexId(), new DoubleWritable(distance));
      }
    }

    vertex.voteToHalt();

  }

  /**
   * This method gives instruction to kill the process and simulate failure.
   *
   * @author Pandu Wicaksono
   * @return instruction to kill this process
   */
  private boolean killProcessEnabled(int superstep){
    boolean result = false;

    boolean attempt = (getContext().getTaskAttemptID().getId() == 0) ? true : false;
    boolean superstep_to_kill = (getSuperstep() == superstep) ? true : false;
    boolean failed_worker = (getWorkerContext().getMyWorkerIndex() == 0) ? true : false;

    result = (attempt && superstep_to_kill && failed_worker);

    return result;
  }

  /**
   * This method checks whether we need to apply compensation function or not.
   *
   * @author Pandu Wicaksono
   * @return
   */
  private boolean compensationFunctionEnabled(){
    boolean result = false;
	
	int numberOfAttempt = getContext().getTaskAttemptID().getId();
	long superstep = getSuperstep();
	
	System.out.println("Check compensationFunctionEnabled workerID " + getMyWorkerIndex() + 
	" attempt " + numberOfAttempt + " superstep " + superstep);

    // first attempt don't have to apply compensation function
    if(numberOfAttempt == 0){
      return false;
    }

    // aligns the failed superstep with the number of attempt
	
	// for alive worker
    if(getMyWorkerIndex() != 0 && numberOfAttempt == 1 && superstep == 2){
      result = true;
    }
	
	// for failed worker
	if(getMyWorkerIndex() == 0 && numberOfAttempt == 2 && superstep == 2){
      result = true;
    }

    return result;
  }

  private void compensateVertex(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex){
    // for the failed worker, the initial value of all vertex is 0
    if(vertex.getValue().equals(new DoubleWritable(0))){
      System.out.println("Compensate function vertex " + vertex.getId() + " by resetting values");
      vertex.setValue(new DoubleWritable(Double.MAX_VALUE));
      return;
    }

    System.out.println("Compensate function vertex " + vertex.getId() + " by sending message");
    // for the success worker, send messages to their edges
    sendMessageToAllEdges(vertex, vertex.getValue());
  }
}
