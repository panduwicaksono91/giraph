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

import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Demonstrates the basic Pregel shortest paths implementation.
 * Updated with failure simulation and compensation function
 * for optimistic recovery.
 * @author Pandu Wicaksono
 */
@Algorithm(
        name = "Shortest paths",
        description = "Finds all shortest paths from a selected vertex"
)
public class SimpleShortestPathsComputationCustomUnweighted extends BasicComputation<
        IntWritable, DoubleWritable, NullWritable, DoubleWritable> {
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
  private boolean isSource(Vertex<IntWritable, ?, ?> vertex) {
    return vertex.getId().get() == SOURCE_ID.get(getConf());
  }

  @Override
  public void compute(
          Vertex<IntWritable, DoubleWritable, NullWritable> vertex,
          Iterable<DoubleWritable> messages) throws IOException {
    // at superstep 0, initialize all vertex value as max
    if (getSuperstep() == 0) {
      vertex.setValue(new DoubleWritable(Double.MAX_VALUE));
    }

    /** Compensation function */
    if((getConf().getRecoveryMode().equals("o") ||
            getWorkerContext().getRecoveryMethod())
      &&
            compensationFunctionEnabled((int) getSuperstep(),
                    getWorkerContext().getRestartedSuperstep())){

      // for the failed worker, the initial value of all vertex is 0
      // (because when the variable double is initialized, the default value is 0,
      // therefore the mandatory checkpoint has 0 as vertex value,
      // then the value will be updated in the initialization of the vertex - superstep 0)
      if(vertex.getValue().equals(new DoubleWritable(0))){
        vertex.setValue(new DoubleWritable(Double.MAX_VALUE));
      } else {
        // for the alive worker, send messages to their edges
        for (Edge<IntWritable, NullWritable> edge : vertex.getEdges()) {
          double distance = vertex.getValue().get() + 1.0d;

          if (LOG.isDebugEnabled()) {
            LOG.debug("Vertex " + vertex.getId() + " sent to " +
                    edge.getTargetVertexId() + " = " + distance);
          }

          sendMessage(edge.getTargetVertexId(), new DoubleWritable(distance));
        }
      }
    }

    /** Failure simulation */
    if(killProcessEnabled(getConf().getSuperstepToKill(), getConf().getWorkerToKill())){
      System.exit(-1);
    }

    /** Algorithm computation */
    // initialize the minimum distance
    double minDist = isSource(vertex) ? 0d : Double.MAX_VALUE;
    for (DoubleWritable message : messages) {
      minDist = Math.min(minDist, message.get());
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Vertex " + vertex.getId() + " got minDist = " + minDist +
              " vertex value = " + vertex.getValue());
    }

    // if the minimum distance is lower than the previous superstep
    // update the value and send message to the neighbour
    if (minDist < vertex.getValue().get()) {
      vertex.setValue(new DoubleWritable(minDist));
      for (Edge<IntWritable, NullWritable> edge : vertex.getEdges()) {
        double distance = minDist + 1;

        if (LOG.isDebugEnabled()) {
          LOG.debug("Vertex " + vertex.getId() + " sent to " +
                  edge.getTargetVertexId() + " = " + distance);
        }

        sendMessage(edge.getTargetVertexId(), new DoubleWritable(distance));
      }
    }

    // always vote to halt after processing this vertex
    vertex.voteToHalt();
  }

  /**
   * This method gives flag to kill the process and simulate failure.
   *
   * @author Pandu Wicaksono
   * @return flag to kill this process
   */
  private boolean killProcessEnabled(String superstepToKill, String workerToKill){
    boolean result = false;

    // superstep to kill
    // parse the string
    String superstepToKillArray[] = superstepToKill.split(",");

    // default value, no superstep to kill, return false
    if(superstepToKillArray.length == 1 && superstepToKillArray[0].equals("")){
      return false;
    }

    // if the worker has already been killed in this superstep, return false
    if((int)getSuperstep() == getWorkerContext().getRestartedSuperstep()){
      return false;
    }

    // parse into integer
    List<Integer> superstepToKillList = new ArrayList<Integer>();
    for(int ii = 0; ii < superstepToKillArray.length; ii++){
      superstepToKillList.add(Integer.parseInt(superstepToKillArray[ii]));
    }

    // worker to kill
    String workerToKillArray[] = workerToKill.split(",");

    // default value, no worker to kill, return false
    if(workerToKillArray.length == 1 && workerToKillArray[0].equals("")){
      return false;
    }

    // parse into integer
    List<Integer> workerToKillList = new ArrayList<Integer>();
    for(int ii = 0; ii < workerToKillArray.length; ii++){
      workerToKillList.add(Integer.parseInt(workerToKillArray[ii]));
    }

    boolean superstep_to_kill =
            (superstepToKillList.contains((int)getSuperstep())) ? true : false;
    boolean failed_worker =
            (workerToKillList.contains(getWorkerContext().getMyWorkerIndex())) ? true : false;
    result = (superstep_to_kill && failed_worker);

    return result;
  }

  /**
   * This method checks whether we need to apply compensation function or not.
   *
   * @author Pandu Wicaksono
   * @return flag to enable compensation function
   */
  private boolean compensationFunctionEnabled(int superstep, int restartedSuperstep){
    boolean result = false;

    // if this is a restarted superstep, apply compensation function
    if(superstep != 0 && superstep == restartedSuperstep){
      return true;
    }

    return result;
  }
}