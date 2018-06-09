/*

Derby - Class org.apache.derbyTesting.perf.clients.BackToBackLoadGenerator

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

package org.apache.derbyTesting.perf.clients;

import java.io.PrintStream;
import java.sql.SQLException;

/**
 * Load generator which creates back-to-back load. This means that you have a
 * number of threads running in parallel, where each thread continuously
 * performs operations with no pauses in between.
 */
public class BackToBackLoadGenerator implements LoadGenerator {
    /** The threads executing. */
    private ClientThread[] threads;
    /** Flag which tells the generator to stop. */
    private volatile boolean stop;
    /** Flag which tells the generator to collect results. */
    private volatile boolean collect;
    /** Start time for steady-state phase. */
    private long startTime;
    /** Stop time for steady-state phase. */
    private long stopTime;

    /**
     * Thread class which runs a single client and collects results.
     */
    private class ClientThread extends Thread {
        private final Client client;
        private long count;

        ClientThread(Client c) {
            client = c;
        }

        public Client getClient() { return client; }

        public void run() {
            try {
                while (!stop) {
                    client.doWork();
                    if (collect) {
                        count++;
                    }
                }
            } catch (SQLException sqle) {
                sqle.printStackTrace();
            }
        }
    }

    /**
     * Initialize the load generator.
     *
     * @param clients the test clients to use
     */
    public void init(Client[] clients) {
        threads = new ClientThread[clients.length];
        for (int i = 0; i < clients.length; i++) {
            threads[i] = new ClientThread(clients[i]);
        }
    }

    /**
     * Start warmup.
     */
    public void startWarmup() {
        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }
    }

    /**
     * Start steady state.
     */
    public void startSteadyState() {
        startTime = System.currentTimeMillis();
        collect = true;
    }

    /**
     * Stop the load generator.
     */
    public void stop() {
        stopTime = System.currentTimeMillis();
        collect = false;
        stop = true;
        for (int i = 0; i < threads.length; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }
        }
    }

    /**
     * Print average number of transactions per second.
     */
    public void printReport(PrintStream out) {
        long time = stopTime - startTime;

        long count = 0;
        for (int i = 0; i < threads.length; i++) {
            count += threads[i].count;
        }

        double tps = (double) count * 1000 / time;

        out.println("Number of threads:\t" + threads.length);
        out.println("Test duration (s):\t" + ((double) time / 1000));
        out.println("Number of transactions:\t" + count);
        out.println("Average throughput (tx/s):\t" + tps);
        
        for (int i = 0; i < threads.length; i++)
        {
            threads[i].getClient().printReport( out );
        }
    }
}
