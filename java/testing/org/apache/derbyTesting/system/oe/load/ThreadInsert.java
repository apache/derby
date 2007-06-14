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
package org.apache.derbyTesting.system.oe.load;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.derbyTesting.system.oe.client.Load;

/**
 * Multi-threaded version of SimpleInsert.
 * A number of threads execute the INSERT statements
 * (each with their own connection).
 * All the rows for a given warehouse are executed by
 * one thread. The threads each get a warehouse to
 * complete and then insert all the rows. Then they
 * loop back to get the next warehouse. Warehouses
 * are assigned to threads in a first come first served
 * fashion. The main thread also inserts the ITEM table.
 * <BR>
 * By default the number of threads is the number
 * of cpus on the machine, unless the scale to be loaded
 * is larger than the number of threads. Then the number
 * of threads will be equal to the scale. If the scale
 * is one or the number of threads is one then the
 * load is just like SimpleInsert.
 * <BR>
 * The number of threads can be set but if it
 * it larger than the scale then the number of threads
 * will be equal to the scale.
 * 
 * <BR>
 * It is assumed that foreign key constraints are
 * created after the load.
 *
 */
public class ThreadInsert extends SimpleInsert implements Runnable  {
    
    /**
     * The main ThreadInsert, created by the main
     * thread and holds the valid nextWarehouse.
     */
    private final ThreadInsert master;
    
    private SQLException loadExceptions;
    
    private int threadCount;
    
    private DataSource ds;
 
    
    /**
     * Next warehouse that needs to be populated.
     */
    private short nextWarehouse = 1;

    /**
     * Create a ThreadInsert loader.
     * @param ds getConnection() will be used to create connections
     * for the spaened threads.
     */
    public ThreadInsert(DataSource ds) {
        super();
        master = this;
        this.ds = ds;
    }
    private ThreadInsert(ThreadInsert master) {
        super();
        this.master = master;
    }
    
    /**
     * Initialize the load by calling the super-class's method
     * and default the number of threads to the smaller of the number
     * of cpus and the scale.
     */
    public void setupLoad(Connection conn, short scale) throws SQLException {
        
        super.setupLoad(conn, scale);

        int cpuCount = Runtime.getRuntime().availableProcessors();
        
        setThreadCount(cpuCount);
    }

    /**
     * Set the thread count.
     * If the scale is less than the number of threads
     * then the number of threads inserting data will 
     * be equal to the scale.
     */
    public void setThreadCount(int threadCount) {
        if (scale == 1) {
            this.threadCount = 1;
            return;
        }
        
        if (scale < threadCount) {
            this.threadCount = scale;
            return;
        }
        
        this.threadCount = threadCount;
    }
    
    /**
     * Populate the database.
     */
    public void populateAllTables() throws Exception {
        if (threadCount == 1)
            super.populateAllTables();
        else
            threadPopulate();
    }

    /**
     * Populate the database using multiple threads.
     * The main thread (ie the one calling this method).
     * is one of the threads that will insert the data
     * It will handle the ITEM table by itself and
     * its fair share of the remaining tables.
     * @throws SQLException 
     * @throws InterruptedException 
     *
     */
    private void threadPopulate() throws SQLException, InterruptedException {
        Thread[] threads = new Thread[threadCount - 1];
        for (int t = 1; t < threadCount; t++)
        {
            // Need to open a new connection.
            Connection loaderConn = ds.getConnection();
           
            ThreadInsert ti = new ThreadInsert(this);
            ti.setSeed((seed * t) / 17);
            ti.setupConnection(loaderConn, scale);
            ti.setRandomGenerator();
            
            Thread loader = new Thread(ti, "OELoader:"+t);
            threads[t - 1] = loader;
            loader.start();
        }

        // load item table
        itemTable(1, Load.ITEM_COUNT);
        
        // and my share of the warehouse based tables.
        run();
        
        for (int j = 0; j < threads.length; j++)
        {
            threads[j].join();
        }
        
        synchronized (this) {
            if (loadExceptions != null)
                throw loadExceptions;
        }
   }
    
    /**
     * Get the next warehouse to populate.
     * If all have been populated then -1 is returned.
     * Warehouse is one based.
     */
    synchronized short getNextWarehouse() {
        short next = nextWarehouse++;
        if (next > scale)
            return -1;
        return next;
    }
    
    /**
     * Save all the exceptions seen during load.
     */
    synchronized void addException(SQLException sqle)
    {
        if (loadExceptions == null)
            loadExceptions = sqle;
        else
        {
            SQLException chain = loadExceptions;
            for (;;)
            {
                SQLException e = chain.getNextException();
                if (e != null) {
                    chain = e;
                    continue;
                }
                chain.setNextException(sqle);
                break;
            }              
        }      
    }
    
    /**
     * Run the load for a thread. Loop insert the data
     * for a single warehouse while there are warehouses
     * left to do.
     */
    public void run() {
        
        short w;
        while ((w = master.getNextWarehouse()) != -1)
        {
            try {
                populateForOneWarehouse(w);
            } catch (SQLException e) {
                master.addException(e);
                break;
            }
        }
    }
}
