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
import org.apache.giraph.utils.HybridUtils;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.IllegalArgumentException;
import java.lang.IllegalStateException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Demonstrates the basic Pregel shortest paths implementation.
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
    if (getSuperstep() == 0) {
      vertex.setValue(new DoubleWritable(Double.MAX_VALUE));
    }

    if(compensationFunctionEnabled()){
//      System.out.println("Compensate function in superstep " + getSuperstep()
//              + " at attempt " + getContext().getTaskAttemptID().getId());

      // for the failed worker, the initial value of all vertex is 0
      if(vertex.getValue().equals(new DoubleWritable(0))){
//        System.out.println("Compensate function vertex " + vertex.getId() + " by resetting values");
        vertex.setValue(new DoubleWritable(Double.MAX_VALUE));
      } else {

//        System.out.println("Compensate function vertex " + vertex.getId() + " by sending message");
        // for the success worker, send messages to their edges

        for (Edge<IntWritable, NullWritable> edge : vertex.getEdges()) {
          double distance = vertex.getValue().get() + 1.0d;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Vertex " + vertex.getId() + " sent to " +
                    edge.getTargetVertexId() + " = " + distance);
          }

//        System.out.println(getSuperstep() + " " + getMyWorkerIndex() + " " + getConf().getLocalHostname()
//            + " " + getConf().getTaskPartition() + " " +
//            "Vertex " + vertex.getId() + " sent to " +
//            edge.getTargetVertexId() + " = " + distance);
          try {
            sendMessage(edge.getTargetVertexId(), new DoubleWritable(distance));
          } catch (IllegalArgumentException e){

          } catch (IllegalStateException e){

          }

        }
      }
    }

    // check whether to kill this process or not
    if(killProcessEnabled(getConf().getSuperstepToKill())){
//      System.out.println("Kill process in superstep " + getSuperstep()
//              + " at attempt " + getContext().getTaskAttemptID().getId());
      HybridUtils.markKillingProcess(getConf().getHybridHomeDir(), (int)getSuperstep());
      try {
        TimeUnit.SECONDS.sleep(30);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
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
//    System.out.println(getSuperstep() + " " + getMyWorkerIndex() + " " + getConf().getLocalHostname()
//            + " " + getConf().getTaskPartition() + " " +
//            "Vertex " + vertex.getId() + " got minDist = " + minDist +
//            " vertex value = " + vertex.getValue());

    if (minDist < vertex.getValue().get()) {
      vertex.setValue(new DoubleWritable(minDist));
      for (Edge<IntWritable, NullWritable> edge : vertex.getEdges()) {
        double distance = minDist + 1;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Vertex " + vertex.getId() + " sent to " +
                  edge.getTargetVertexId() + " = " + distance);
        }

//        System.out.println(getSuperstep() + " " + getMyWorkerIndex() + " " + getConf().getLocalHostname()
//                + " " + getConf().getTaskPartition() + " " +
//                "Vertex " + vertex.getId() + " sent to " +
//                edge.getTargetVertexId() + " = " + distance);
        try {
          sendMessage(edge.getTargetVertexId(), new DoubleWritable(distance));
        } catch (IllegalArgumentException e){

        } catch (IllegalStateException e){

        }
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
  private boolean killProcessEnabled(String superstepToKill){
    boolean result = false;

//    System.out.println("Check killProcessEnabled");
//    System.out.println("superstepToKill: " + superstepToKill);

    // parse the string
    String superstepToKillArray[] = superstepToKill.split(",");

    // default value
    if(superstepToKillArray.length == 1 && superstepToKillArray[0].equals("")){
//	  System.out.println("superstepToKillArray[0]: " + superstepToKillArray[0]);
//	  System.out.println("no need to kill");
      return false;
    }

//    System.out.println("Check list");

    // parse into integer
    List<Integer> superstepToKillList = new ArrayList<Integer>();
    for(int ii = 0; ii < superstepToKillArray.length; ii++){
      superstepToKillList.add(Integer.parseInt(superstepToKillArray[ii]));
//	  System.out.println(superstepToKillList.get(ii));
    }

    int index = superstepToKillList.indexOf((int)getSuperstep());
//    System.out.println("Check killProcessEnabled index: " + index + " superstep: " + getSuperstep());
//    int numOfAttempt = getContext().getTaskAttemptID().getId();

//	  System.out.println("Check equal superstep: " + superstepToKillList.contains((int)getSuperstep()));

    boolean attempt = (!HybridUtils.checkKillingProcess(getConf().getHybridHomeDir(),(int)getSuperstep()))
            ? true : false;
    boolean superstep_to_kill = (superstepToKillList.contains((int)getSuperstep())) ? true : false;
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

//    int numberOfAttempt = getContext().getTaskAttemptID().getId();
//    long superstep = getSuperstep();

//    System.out.println("Check compensationFunctionEnabled workerID " + getMyWorkerIndex() +
//    " attempt " + numberOfAttempt + " superstep " + superstep);

    if(HybridUtils.checkKillingProcess(getConf().getHybridHomeDir(),(int)getSuperstep())){
      result = true;
    }

    return result;

    // first attempt don't have to apply compensation function
//    if(numberOfAttempt == 0){
//      return false;
//    }


//    // aligns the failed superstep with the number of attempt
//    // parse the string
//    String superstepToKillArray[] = getConf().getSuperstepToKill().split(",");
//
//    // parse into integer
//    List<Integer> superstepToKillList = new ArrayList<Integer>();
//    for(int ii = 0; ii < superstepToKillArray.length; ii++){
//      superstepToKillList.add(Integer.parseInt(superstepToKillArray[ii]));
//    }
//
//    int index = superstepToKillList.indexOf((int)superstep);
//
//    // for alive worker
//    if(getMyWorkerIndex() != 0 && ((index + 1) == numberOfAttempt)){
//      result = true;
//    }
//
//    // for failed worker
//    if(getMyWorkerIndex() == 0 && ((index + 1)*2 == numberOfAttempt)){
//      result = true;
//    }
  }
}