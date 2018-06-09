/*
 *
 * Derby - Class org.apache.derbyTesting.system.oe.client.MultiThreadSubmitter
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
package org.apache.derbyTesting.system.oe.client;

/**
 * Execute transactions using multiple threads.
 * A single thread uses a single submitter,
 * submitters are created outside of this class.
 */
public class MultiThreadSubmitter {

    /**
     * Execute count transactions per submitter
     * using a newly created thread for each
     * submitter. In total (count*submitter.length)
     * transactions will be executed. The time returned
     * will be the time to execute all the transactions.
     * 
     * Each submitter will have its clearTransactionCount called
     * before the run.
     * 
     * @param submitters Submitters to use.
     * @param displays Displays for each submitter.
     * If null then null will be passed into each transaction
     * execution
     * @param count Number of transactions per thread.
     * @return Time to excute all of the transactions.
     */
    public static long multiRun(
            Submitter[] submitters,
            Object[] displays,
            int count) {

        Thread[] threads = new Thread[submitters.length];
        for (int i = 0; i < submitters.length; i++) {
            submitters[i].clearTransactionCount();
            Object displayData = displays == null ? null : displays[i];
            threads[i] = newThread(i, submitters[i], displayData, count);
        }

        // Start all the threads
        long start = System.currentTimeMillis();
        for (int i = 0; i < threads.length; i++)
            threads[i].start();

        // and then wait for them to finish
        for (int i = 0; i < threads.length; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        long end = System.currentTimeMillis();

        return end - start;
    }

    /**
     * Return a thread that will run count transactions using a submitter.
     * 
     * @param threadId
     *            Number of thread.
     * @param submitter
     *            Submitter
     * @param displayData
     *            DisplayData for this submitter
     * @param count
     *            Number of transactions to run.
     * 
     * @return Thread (not started)
     */
    private static Thread newThread(final int threadId,
            final Submitter submitter,
            final Object displayData, final int count) {
        Thread t = new Thread("OE_Thread:" + threadId) {

            public void run() {
                try {
                    submitter.runTransactions(displayData, count);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        return t;
    }

}
