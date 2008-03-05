/*
 *
 * Derby - Class JDBCPerfTestCase
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
package org.apache.derbyTesting.junit;


/**
 * Base Class for performance tests. It is useful for a performance test
 * framework to have ability to run a test for I iterations , and repeat the
 * test R times. This class provides this functionality of timing the test for I
 * iterations and repeating the test in the same jvm for R number of
 * times. 
 * 
 * For JDBCPerfTestCase with R repeats and I iterations
 * 
 * This cycle is repeated R times 
 * {
 *    setUp run once 
 *    fixture method is run I times(and timed) 
 *    tearDown is run once
 * }
 *
 * 
 * Results are printed out to System.out currently. For each repeat of the test, 
 * the elapsed time is printed out and after R repeats of the test, the average
 * elapsed time is also printed.
 *  
 * If  a test has R repeats and (R>1), then the average elapsed time of the
 * (R-1) runs is printed out and the timing info collected as part of the first 
 * testrun is ignored. 
 * If R=1, then the average elapsed time prints time for that
 * only run. 
 * 
 * To see the current output that is printed out, see methods runBare() and 
 * runTest()
 * 
 * To write a performance test, extend the JDBCPerfTestCase 
 * In the example below, ScanCoveredIdxTest is a performance test that 
 * extends JDBCPerfTestCase. See below code snippet on how 
 * these tests will be added/used.  
 * <CODE>
 *  // Add a test fixture 'scanAllRows' in ScanCoveredIdxTest 
 *  // and to run this test for 100 iterations and to 
 *  // repeat the test 4 times.
 *  int iterations = 100; 
 *  int repeats = 4; 
 *  TestSuite suite = new TestSuite();
 *  suite.addTest(new ScanCoveredIdxTest("scanAllRows",iterations,repeats));
 * 
 *  // To add client tests. 
 * TestSuite client = new TestSuite("Client");
 * client.addTest(new ScanCoveredIdxTest("scanAllRows",iterations,repeats));
 * client.addTest(new ScanCoveredIdxTest("scanAndRetrieveAllRows",iterations,repeats));
 * 
 * // This will add the server decorator that will start the
 * // server on setUp of the suite and stop server on tearDown 
 * // of the suite.
 * suite.addTest(TestConfiguration.clientServerDecorator(client));
 * </CODE>
 * 
 * Some improvement areas/ideas: 
 * -- Can we use TestResult ,and our own TestRunner to improve on how the 
 * results are reported.  
 * -- write the perf results to a file that can be easily consumed for reporting purposes
 * and further analysis against different builds.
 * -- Maybe even write the results out in xml format,and then using xsl the results
 * could be rendered into html reports
 */
public class JDBCPerfTestCase extends BaseJDBCTestCase {
    
    /**
     * store timing information
     */
    private long startTime,endTime;
    
    /**
     * Store info on how many times the test fixture should be run
     * also see runTest() on how iterations is used 
     */
    private int iterations = 1;
    
    /**
     * Store info on how many times should the test be repeated
     * default value is 1
     */
    private int repeats = 1;
    
    /**
     * Hold the elapsedtime info for the testRun
     * given by testRunNum
     */
    private int testRunNum = 0;
    
    /**
     * store the elapsed time info for the test runs.
     */
    private long[] runs;
    
    
    public JDBCPerfTestCase(String name)
    {
        super(name);
        runs = new long[repeats];
    }
    
    /**
     * @param name testname
     * @param iterations iterations of the test to measure at one shot.
     * @param repeats is the number of times the entire test be repeated
     * @see JDBCPerfTestCase  class level comments
     */
    public JDBCPerfTestCase(String name,int iterations, int repeats)
    {
        super(name);
        this.iterations = iterations;
        this.repeats = repeats;
        runs = new long[repeats];

    }
    
  
    /**
     * runBare, do the whole thing - setup, runTest, cleanup
     * 'repeats' times.
     * If test was run for more than one time, then ignore the 
     * first testrun results and print the average elapsed time 
     * for the remaining runs.  
     */
    public void runBare() throws Throwable
    {
        for (int i = 0; i < repeats; i++)
        {
            println("Repeat ="+i);
            super.runBare();
            testRunNum++;
        }
        
        // For a single run no point in printing a summary that's
        // identical to the one run output.
        if (repeats == 1 && iterations == 1)
            return;
        
        long total=0;
        
        if ( repeats > 1) 
            for (int i = 1; i < repeats; i++)
                total += runs[i];
        else
            total = runs[0];

        System.out.println("Test-" + getName() +
                ": framework:"+ getTestConfiguration().getJDBCClient().getName()+
                ":iterations: " + iterations
                + " : Avg elapsedTime(ms): " + 
                total / (repeats > 1 ? (long) (repeats - 1) : (long) repeats));
    }
    
    /**
     * Overrides runTest from TestCase, in order to gather 
     * elapsed time information. 
     * Run the testfixture for 'iterations' times and store the time 
     * taken for the particular testrun and print it to System.out
     * Note: this method will NOT time the setUp or tearDown methods 
     */
    protected void runTest() throws Throwable {
        startTime = System.currentTimeMillis();
        for (int j = 0; j < iterations; j++) 
            super.runTest();
        endTime = System.currentTimeMillis();
        runs[testRunNum] = (endTime-startTime);
        System.out.println("Test-" + getName() + 
                ": framework:"+ getTestConfiguration().getJDBCClient().getName()+
                ": run#" + testRunNum
                + " iterations: " + iterations + " : elapsedTime(ms): "
                + (endTime - startTime));
    }
}
