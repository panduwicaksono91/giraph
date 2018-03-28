package org.apache.giraph.examples;

import org.apache.giraph.worker.WorkerContext;

import org.apache.log4j.Logger;

public class SimpleShortestPathsComputationCustomWorkerContext extends WorkerContext {

    static double failed_superstep = 2;

    private static final Logger LOG = Logger.getLogger(SimpleShortestPathsComputationCustomWorkerContext.class);

    @Override
    public void preApplication() throws InstantiationException, IllegalAccessException {

    }

    @Override
    public void postApplication() {

    }

    @Override
    public void preSuperstep() {

    }

    @Override
    public void postSuperstep() {

    }
}
