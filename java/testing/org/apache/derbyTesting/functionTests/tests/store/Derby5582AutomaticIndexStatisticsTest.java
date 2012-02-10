/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.store.Derby5582AutomaticIndexStatisticsTest

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.SQLException;
import java.util.ArrayList;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

public class Derby5582AutomaticIndexStatisticsTest extends AutomaticIndexStatisticsTest  {

    // private thread group. Derby5582SecurityManager will prevent other threads from 
	// modifying this thread group.
    private static final String PRIVTGNAME = "privtg";

	public Derby5582AutomaticIndexStatisticsTest(String name) {
        super(name);
        
    }
    
    /**
     * DERBY-5582 Ensure automatic statistics update thread can be created in the 
     * context of a SecurityManager that disallows modification of the parent 
     * thread thread group.
     * 
     * @throws InterruptedException
     */
    public void testDerby5582() throws InterruptedException {
        //Create a new thread belonging to the thread group protected by 
        // the custom SecurityManger
        ThreadGroup privtg = new ThreadGroup(PRIVTGNAME);
        // Derby5582Runner will run a automatic statistics test within
        // the context of the "privtg" ThreadGroup
        Derby5582Runner runner = new Derby5582Runner();
        Thread t = new Thread(privtg, runner, "runner-thread");
        t.start();
        t.join();
        // Report error if any during run
        Exception error = runner.getSavedError();
        if (error != null) {
            fail(error.getMessage(),error);
        }
        
    }
   
   
    
    public static Test suite() {
    	// Run just the one fixture with the custom SecurityManager
        Test t = new Derby5582AutomaticIndexStatisticsTest("testDerby5582");
        Derby5582SecurityManager sm =  new Derby5582SecurityManager();
        return TestConfiguration.additionalDatabaseDecorator(new SecurityManagerSetup(t, null,
                sm),MASTERDB);
        }

    /**
     * SecurityManager which prevents modification of thread group privtg
     *
     */
    public static class Derby5582SecurityManager  extends SecurityManager {
        
        public void checkAccess(ThreadGroup tg) {
            ThreadGroup currentGroup = Thread.currentThread().getThreadGroup();
            if (tg.getName().equals(PRIVTGNAME) && 
                    !currentGroup.getName().equals("main")) {
                throw new SecurityException("No permission to private ThreadGroup privtg");
                
            }
            super.checkAccess(tg);
        }
    }
    
    /**
     * Runnable to run testSTatsUpdatedOnGrowthFixture from
     * AutomaticStatisticsTest. Needs to be run in a separate thread
     * with disallowed ThreadGroup modification
     *  
     */
    public class Derby5582Runner implements Runnable {

    	// error saved from run so it can be reported in 
    	// fixture as failure.
    	private Exception savedError = null;

    	public void run() {
    		try {
    			testStatsUpdatedOnGrowth();
    		} catch (SQLException sqle) {
    			savedError = sqle;
    		}   
    	}

    	/**
    	 * @return saved Error
    	 */
    	public Exception getSavedError() {
    		return savedError;
    	}

    }
}
