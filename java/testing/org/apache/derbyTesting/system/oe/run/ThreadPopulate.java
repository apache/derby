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
