/*
 * Derby - Class org.apache.derbyTesting.functionTests.util.Barrier
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package org.apache.derbyTesting.functionTests.util;

import junit.framework.Assert;

/**
 * In the absence of java.util.concurrent.CyclicBarrier on some of the
 * platforms we test, create our own barrier class. This class allows
 * threads to wait for one another on specific locations, so that they
 * know they're all in the expected state.
 */
public class Barrier {
    /** Number of threads to wait for at the barrier. */
    private int numThreads;

    /** Create a barrier for the specified number of threads. */
    public Barrier(int numThreads) {
        this.numThreads = numThreads;
    }

    /**
     * Wait until {@code numThreads} have called {@code await()} on this
     * barrier, then proceed.
     *
     * @throws InterruptedException if the thread is interrupted while
     * waiting for the other threads to reach the barrier.
     */
    public synchronized void await() throws InterruptedException {
        Assert.assertTrue(
                "Too many threads reached the barrier", numThreads > 0);

        if (--numThreads <= 0) {
            // All threads have reached the barrier. Go ahead!
            notifyAll();
        }

        // Some threads haven't reached the barrier yet. Let's wait.
        while (numThreads > 0) {
            wait();
        }
    }
}
