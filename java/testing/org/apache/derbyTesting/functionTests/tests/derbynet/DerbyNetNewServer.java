/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.DerbyNetNewServer

   Copyright 2003, 2005 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.functionTests.tests.derbynet;

import org.apache.derby.drda.NetworkServerControl;

import org.apache.derbyTesting.functionTests.harness.jvm;
import org.apache.derby.tools.ij;

import java.util.Properties;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.derbyTesting.functionTests.util.TestUtil;

/**
 * Test NetworkServerControl.start(PrintWriter) writes to the print Writer
 *
 * test:
 *<ul>
 *<li> start( printWriter)
 *<li> start( (PrintWriter) null)
 *</ul>
 */

public class DerbyNetNewServer extends Thread
{

    private static final String DATABASE_NAME = "wombat";
    private static boolean passed = true;
    private static final Properties authenticationProperties;
    static
    {
        authenticationProperties = new Properties();
        authenticationProperties.put ("user", "admin");
        authenticationProperties.put ("password", "admin");
    }

    private NetworkServerControl server;
    
    public static void main( String[] args)
    {
        try
        {
			TestUtil.loadDriver();
            Class.forName( "org.apache.derby.jdbc.EmbeddedDriver").newInstance();
            
			ij.getPropertyArg(args);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            testServer(new NetworkServerControl(), bos, "non-null PrintWriter");
            testServer(new NetworkServerControl(), null, "null PrintWriter");
        }
        catch( Exception e)
        {
            e.printStackTrace();
            passed = false;
        }
        if( passed)
            System.out.println( "PASSED.");
        else
            System.out.println( "FAILED.");
    }

    private static void testServer( NetworkServerControl server, 
									ByteArrayOutputStream bos, String label)
		throws Exception
    {
		PrintWriter writer = null;

        System.out.println( "Testing " + label);
        if( bos != null)
		{
            bos.reset();
            // DERBY-1466, Test that messages are flushed to the
            // writer irrespective of whether the user's writer is
            // set to autoflush true.
            writer = new PrintWriter(bos); 
		}
		server.start(writer);
        Connection conn = null;
        
        // Wait for it to start
        for( int ntries = 1;; ntries++)
        {
            try
            {
                Thread.sleep(500);
            }
            catch( InterruptedException ie){};
            
            try
            {
                conn = DriverManager.getConnection(TestUtil.getJdbcUrlPrefix()
												   + DATABASE_NAME + 
												   ";create=true",
                                                    authenticationProperties);

                break;
            }
            catch( SQLException sqle)
            {
                if( ntries > 10)
                {
                    System.out.println( "Server start failed: " + sqle.getMessage());
                    if( bos != null)
                    {
                        System.out.println( "Server log:");
                        System.out.println( bos.toString());
                    }
                    passed = false;
                    break;
                }
            }
        }
        if( conn != null)
        {
            try
            {
                conn.close();
            }
            catch( SQLException sqle)
            {
                passed = false;
                System.out.println( "SQLException thrown in close: " + sqle.getMessage());
            }
        }
        try
        {
            server.shutdown();
        }
        catch( Exception e)
        {
            passed = false;
            System.out.println( "Server shutdown failed: " + e.getMessage());
        }

        if( bos != null)
        {
            if( bos.size() == 0)
            {
                passed = false;
                System.out.println( "Nothing written to the server log.");
            }
        }
    } // end of testServer

    private DerbyNetNewServer( NetworkServerControl server)
    {
        this.server = server;
    }

}
