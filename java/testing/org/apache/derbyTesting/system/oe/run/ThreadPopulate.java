/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to you under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.derbyTesting.system.oe.run;

import junit.framework.Test;

import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.system.oe.load.ThreadInsert;

/**
 * Version of Populate that uses the multi-threaded loader.
 */
public class ThreadPopulate extends Populate {

    public ThreadPopulate(String name) {
        super(name);
     }
    /**
     * Run OE load
     * @param args supply arguments for benchmark.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception
    {
        parseArgs(args);
        String[] tmp= {"org.apache.derbyTesting.system.oe.run.ThreadPopulate"};
        
        // run the tests.
        junit.textui.TestRunner.main(tmp);
    }
    /**
     * Setup the ThreadInsert loader.
     */
    protected void setUp() throws Exception {
       loader = new ThreadInsert(JDBCDataSource.getDataSource());
       loader.setupLoad(getConnection(), scale);
    }
    
    public static Test suite() {
        return loaderSuite(ThreadPopulate.class);
    }

}
