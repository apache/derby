/*

Derby - Class org.apache.derbyTesting.perf.clients.PoissonLoadGenerator

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
import java.util.Random;

/**
 * Load generator which generates Poisson distributed requests.
 */
public class PoissonLoadGenerator implements LoadGenerator {

    /**
     * The average time (in milliseconds) between each request from a client.
     */
    private final double avgWaitTime;

    /** The client threads used to generate load. */
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
     * Construct a {@code PoissonLoadGenerator} instance.
     *
     * @param avgWaitTime the time (in millisecond) between each request from
     * a client
     */
    public PoissonLoadGenerator(double avgWaitTime) {
        this.avgWaitTime = avgWaitTime;
    }

    /**
     * Thread class which runs a single client and collects results.
     */
    private class ClientThread extends Thread {
        private final Client client;
        private long count;
        private long totalTime;
        private long min = Long.MAX_VALUE;
        private long max = 0;

        ClientThread(Client c) {
            client = c;
        }

        /**
         * Tell the client to stop waiting.
         */
        synchronized void wakeup() {
            ClientThread.this.notifyAll();
        }

        public void run() {
            try {
                runClient();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void runClient() throws SQLException, InterruptedException {
            final Random r = new Random();
            final long start = System.currentTimeMillis();
            // use a double to prevent too short average wait time because
            // of truncation
            double timeOffset = 0d;
            long now = start;
            while (!stop) {
                // Increase the timeOffset by a random value picked from an
                // exponential distribution (exponentially distributed wait
                // times give Poisson distributed requests, see
                // http://en.wikipedia.org/wiki/Exponential_distribution)
                timeOffset += -Math.log(r.nextDouble()) * avgWaitTime;
                final long nextWakeup = start + (long) timeOffset;
                while (now < nextWakeup) {
                    synchronized (ClientThread.this) {
                        if (stop) {
                            return;
                        }
                        ClientThread.this.wait(nextWakeup - now);
                    }
                    now = System.currentTimeMillis();
                }

                final long t0 = now;
                client.doWork();
                final long t1 = System.currentTimeMillis();
                if (collect) {
                    final long time = t1 - t0;
                    count++;
                    totalTime += time;
                    if (time > max) max = time;
                    if (time < min) min = time;
                }
                now = t1;
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
     * Start warmup phase.
     */
    public void startWarmup() {
        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }
    }

    /**
     * Start steady-state phase.
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
                threads[i].wakeup();
                threads[i].join();
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }
        }
    }

    /**
     * Print average transaction injection rate and response times.
     */
    public void printReport(PrintStream out) {
        long time = stopTime - startTime;

        long count = 0;
        long totalTime = 0;
        long min = Long.MAX_VALUE;
        long max = 0;
        for (int i = 0; i < threads.length; i++) {
            count += threads[i].count;
            totalTime += threads[i].totalTime;
            min = Math.min(min, threads[i].min);
            max = Math.max(max, threads[i].max);
        }

        double tps = (double) count * 1000 / time;
        double avgResp = (double) totalTime / count;

        out.println("Number of threads:\t" + threads.length);
        out.println("Average injection rate (tx/s):\t" + tps);
        out.println("Average response time (ms):\t" + avgResp);
        out.println("Minimum response time (ms):\t" + min);
        out.println("Maximum response time (ms):\t" + max);
    }
}
