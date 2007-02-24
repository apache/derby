/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.DerbyNetAutoStart

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

package org.apache.derbyTesting.functionTests.tests.derbynet;

import org.apache.derby.iapi.reference.Property;
import org.apache.derby.drda.NetworkServerControl;

import org.apache.derbyTesting.functionTests.harness.jvm;
import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.TestUtil;

import java.io.File;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.io.FileReader;
import java.io.FileInputStream;

import java.net.InetAddress;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Enumeration;
import java.util.Properties;
import java.util.Vector;

/**
 * Test the network server derby.drda.startNetworkServer property.
 *
 * Test that:
 *<ul>
 *<li>The network server is started when the property value is true.
 *<li>The network server is not started when the property value is false.
 *<li>The default port number is used when the port property is not specified.
 *<li>The server uses a non-default port when a port property is set.
 *<li>A message is printed to derby.log when the server is already started.
 *</ul>
 */
public class DerbyNetAutoStart
{

    protected static boolean passed = true;

    private static final String JUST_START_SERVER_ARG = "justStartServer=";
    private static Connection drdaConn;
    private static Connection embeddedConn;
    private static int testNumber = 0;
    private static int portNumber;
    private static String hostName;
    private static String homeDir;
    private static String databaseName;
    private static Properties baseProperties = new Properties();
    private static StringBuffer basePropertiesSB = new StringBuffer();
    private static File derbyPropertiesFile;
    private static final Properties authenticationProperties;
    static
    {
        authenticationProperties = new Properties();
        authenticationProperties.put ("user", "admin");
        authenticationProperties.put ("password", "admin");
    }
    private static PrintStream realSystemOut;
    private static ByteArrayOutputStream serverOutputBOS = new ByteArrayOutputStream();
    private static PrintStream serverOutputOut = new PrintStream( serverOutputBOS);
    
    public static void main( String[] args)
    {
        setup( args);
        runAllTests();
        if( passed)
        {
            System.out.println( "PASSED.");
            System.exit(0);
        }
        else
        {
            System.out.println( "FAILED.");
            System.exit(1);
        }
    } // end of main

    protected static void setup( String[] args)
    {
        realSystemOut = System.out;
        try
        {
			TestUtil.loadDriver();

			ij.getPropertyArg(args);
            homeDir = System.getProperty( "derby.system.home", ".");
            hostName = TestUtil.getHostName();
            
            for( int i = 0; i < args.length; i++)
            {
                if( args[i].startsWith( JUST_START_SERVER_ARG))
                {
                    PrintStream out = getPrintStream( homeDir + File.separatorChar + "serverOutput.txt");
                    System.setOut( out);
                    System.setErr( out);

                    Class.forName( "org.apache.derby.jdbc.EmbeddedDriver").newInstance();
                    try
                    {
                        portNumber = Integer.parseInt( args[i].substring( JUST_START_SERVER_ARG.length()));
                    }
                    catch( Exception e)
                    {
                        portNumber = -1; // use the default
                    }
                    if( portNumber <= 0)
                        portNumber = NetworkServerControl.DEFAULT_PORTNUMBER;
                    
                    NetworkServerControl server = new NetworkServerControl(InetAddress.getByName(hostName),portNumber);
                    server.start(null);
					// Wait for server to come up 
					for (int j = 0; j < 60; j++)
					{
						Thread.sleep(1000);
						if (isServerStarted(server))
							break;
					}
					// Block so other process can get connections
					while (isServerStarted(server))
						Thread.sleep(500);
                    System.exit(0);
                }
            }
            
            derbyPropertiesFile = new File( homeDir + File.separatorChar + "derby.properties");

            try
            {
                FileReader propertiesReader = new FileReader( derbyPropertiesFile);
                for( int c; (c = propertiesReader.read()) != -1;)
                    basePropertiesSB.append( (char) c);
                baseProperties.load( new FileInputStream( derbyPropertiesFile));
            }
            catch( IOException ioe){}
        }
        catch( Exception e)
        {
            System.out.println( e.getClass().getName() + " thrown: " + e.getMessage());
            e.printStackTrace();
            passed = false;
        }
        if( ! passed)
            System.exit(1);
    } // end of setup

    protected static void runAllTests()
    {
        testNumber = 0;
        try
        {
            portNumber = NetworkServerControl.DEFAULT_PORTNUMBER;
            if( startTest( new String[] { Property.START_DRDA, "true"}))
            {
                endTest(true);
            }

            portNumber = 31415;
            if( startTest( new String[] { Property.START_DRDA, "true",
                                          Property.DRDA_PROP_PORTNUMBER, String.valueOf( portNumber)}))
            {
                endTest(true);
            }

            portNumber = -1;
            if( startTest( new String[] { Property.START_DRDA, "false"}))
            {
                deleteDir( homeDir + File.separatorChar + databaseName);

                try
                {
                    drdaConn = DriverManager.getConnection( TestUtil.getJdbcUrlPrefix() + databaseName,
                                                            authenticationProperties);
                    passed = false;
                    System.out.println( "  The network server was started though " + Property.START_DRDA
                                        + "=false.");
                }
                catch( SQLException sqle){}
                endTest( false);
            }

            if( startTest( new String[] { }))
            {
                try
                {
                    drdaConn =
						DriverManager.getConnection(TestUtil.getJdbcUrlPrefix()+
													"database" + 
													testNumber,
													authenticationProperties);
                    passed = false;
                    System.out.println( "  The network server was started though " + Property.START_DRDA
                                        + " was not set.");
                }
                catch( SQLException sqle){}
                endTest( false);
            }
            // Start the network server in a different JVM and check that autostart handles it.
            testExtantNetServer();
        }
        catch( Exception e)
        {
            System.out.println( e.getClass().getName() + " thrown: " + e.getMessage());
            e.printStackTrace();
            passed = false;
        }
    }

    private static PrintStream getPrintStream( String fileName)
    {
        try
        {
            return new PrintStream( new FileOutputStream( fileName));
        }
        catch( Exception e)
        {
            System.out.println( "Could not create " + fileName);
            System.out.println( e.getMessage());
            System.exit(1);
            return null;
        }
    } // end of getPrintStream

    private static final String logFileProperty = "derby.stream.error.file";
    
    private static void testExtantNetServer() throws Exception
    {
        RandomAccessFile logFile;
        String portStr;
        String logFileName = homeDir + File.separator + "derby.log";

        announceTest();

        long startLogFileLength = getLogFileLength( logFileName);
        String logAppendProp = System.getProperty( Property.LOG_FILE_APPEND);
        if( logAppendProp == null)
            logAppendProp = baseProperties.getProperty( Property.LOG_FILE_APPEND);
        boolean appendingToLog = ( logAppendProp != null && (new Boolean( logAppendProp).booleanValue()));
        
        if( ! writeDerbyProperties( new String[]{}))
            return;
        
        // Start the network server in another process
        jvm jvm = null;
        try
        {
            jvm = jvm.getCurrentJvm();
        }
        catch( Exception e)
        {

            passed = false;
            System.out.println( " Could not get the current JVM:");
            System.out.println( "   " + e.getMessage());
            return;
        }
        portNumber = -1;
        Vector vCmd = jvm.getCommandLine();
        Properties systemProperties = System.getProperties();
        String cmd[] = new String[ vCmd.size() + systemProperties.size() + 3];
        int i;
        for( i = 0; i < vCmd.size(); i++)
            cmd[i] = (String) vCmd.elementAt(i);
        for( Enumeration e = systemProperties.keys(); e.hasMoreElements();)
        {
            String propName = (String) e.nextElement();
            if( ! propName.equals( logFileProperty))
                cmd[i++] = "-D" + propName + "=" + (String) systemProperties.get( propName);
        }
        cmd[i++] = "-D" + logFileProperty + "=derbynet.log";
        cmd[i++] = "org.apache.derbyTesting.functionTests.tests.derbynet.DerbyNetAutoStart";
        if( portNumber > 0)
        {
            portStr = String.valueOf( portNumber);
            cmd[i++] = JUST_START_SERVER_ARG + ((portNumber > 0) ? String.valueOf( portNumber) : "");
            portStr = ":" + portStr;
        }
        else
        {
            portStr = "1527";
            cmd[i++] = JUST_START_SERVER_ARG;
        }
		/*
		System.out.println("Cmd:");
		for (int c = 0; c < cmd.length;c++)
			System.out.print(cmd[c] + " ");
		System.out.println("");
		*/

        Process serverProcess = Runtime.getRuntime().exec( cmd);
        // Wait for to start
        String dbUrl = TestUtil.getJdbcUrlPrefix(hostName,
												 Integer.parseInt(portStr)) +
			"database1";
        Connection drdaConn = null;
        for( int ntries = 1; ; ntries++)
        {
            try
            {
                Thread.sleep(500);
            }
            catch( InterruptedException ie){};

            try
            {
                drdaConn = DriverManager.getConnection( dbUrl, authenticationProperties);
                break;
            }
            catch( SQLException sqle)
            {
                if( ntries > 20)
                {
                    System.out.println( "Server start failed: " +
										sqle.getMessage());
					sqle.printStackTrace();
                    passed = false;
                    return;
                }
            }

        }


        try
        {
            String[] properties;
            if( portNumber <= 0)
                properties = new String[] {Property.START_DRDA, "true"};
            else
                properties = new String[] {Property.START_DRDA, "true",
                                           Property.DRDA_PROP_PORTNUMBER, 
										   String.valueOf( portNumber)};
            portNumber = -1;
            if( runTest( properties))
            {
                checkConn( drdaConn, "network");
                checkConn( embeddedConn, "embedded");
                drdaConn.close();
                    
                endTest( false);

                // There should be a warning in the derby.log file.
                try
                {
                    // The network server is started in a different thread, so give it a little time
                    // to write the message
                    Thread.sleep(1000);
                    logFile = new RandomAccessFile( logFileName, "r");
                    if( appendingToLog)
                        logFile.seek( startLogFileLength);
                }
                catch( Exception e)
                {
                    System.out.println( "Cannot open derby.log: " + e.getMessage());
                    passed = false;
                    drdaConn.close();
                    stopServer( serverProcess);
                    return;
                }
                if( !checkLog( logFileName, new String[] {"An exception was thrown during network server startup"}))
                {
                    // Was the network server started? Print out the names of the threads
                    System.out.println( "Active threads:");
                    ThreadGroup tg = Thread.currentThread().getThreadGroup();
                    while( tg.getParent() != null)
                        tg = tg.getParent();
                    Thread[] activeThreads = new Thread[ 16*Thread.activeCount()];
                    int threadCount = tg.enumerate( activeThreads, true);
                    for( int idx = 0; idx < threadCount; idx++)
                        System.out.println( "  " + activeThreads[idx].getName());
                    // Is the server process still running?
                    try
                    {
                        int ev = serverProcess.exitValue();
                        System.out.println( "The separate server process exited prematurely with code " + ev);
                    }
                    catch( IllegalThreadStateException itse)
                    {
                        System.out.println( "The server process seems to be running.");
                    }
                }
            }
        }
        catch( Exception e)
        {
            e.printStackTrace();
            passed = false;
        }
        stopServer( serverProcess);
    } // end of testExtantNetServer

    private static long getLogFileLength( String logFileName)
    {
        try
        {
            RandomAccessFile logFile = new RandomAccessFile( logFileName, "r");
            long length = logFile.length();
            logFile.close();
            return length;
        }
        catch( Exception e){ return 0;}
    }

    private static void checkConn( Connection conn, String label)
    {
        try
        {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getSchemas();
            while( rs.next());
            rs.close();
        }
        catch( SQLException sqle)
        {
            passed = false;
            System.out.println( "Could not use the " + label + " connection:");
            System.out.println( "  " + sqle.getMessage());
        }
    } // end of checkConn

    private static void stopServer( Process serverProcess)
    {
        try
        {
            NetworkServerControl server =
				new NetworkServerControl(InetAddress.getByName(hostName),
									 portNumber);
			server.shutdown();
            Thread.sleep(5000);
        }
        catch( Exception e)
        {
            System.out.println( "  Exception thrown while trying to shutdown the remote server.");
            System.out.println( "    " + e.getMessage());
            passed = false;
        }
        serverProcess.destroy();
    } // end of stopServer
        
    private static boolean checkLog( String logFileName, String[] expected) throws IOException
    {
        boolean allFound = true;
        boolean[] found = new boolean[ expected.length];
        FileInputStream is = new FileInputStream(logFileName);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String logLine; 
        while((logLine = br.readLine()) != null)            
        {
            for( int i = 0; i < expected.length; i++)
            {
                if( (! found[i]) && logLine.indexOf( expected[i]) >= 0)
                    found[i] = true;
            }
        }
        for( int i = 0; i < expected.length; i++)
        {
            if( ! found[i])
            {
                passed = false;
                System.out.println( "Derby.log does not contain\n  '" + expected[i] + "'.");
                allFound = false;
            }
        }
        return allFound;
    } // end of checkLog

    private static boolean startTest( String[] properties)
    {
        announceTest();
        return runTest( properties);
    }

    private static boolean runTest( String[] properties)
    {
        drdaConn = null;
        embeddedConn = null;

        if( !writeDerbyProperties( properties))
            return false;
        
        deleteDir( homeDir + File.separatorChar + databaseName);
        try
        {
            System.setOut( serverOutputOut);
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
            embeddedConn = DriverManager.getConnection( "jdbc:derby:" + databaseName + ";create=true");
            System.setOut( realSystemOut);
        }
        catch( SQLException sqle)
        {
            System.setOut( realSystemOut);
            passed = false;
            System.out.println( "  Could not create an embedded database.");
            System.out.println( "    " + sqle.getMessage());
            return false;
        }
        catch( Exception e)
        {
            System.setOut( realSystemOut);
            passed = false;
            System.out.println( "  Could not start the Derby client driver.");
            System.out.println( "    " + e.getMessage());
            return false;
        }

        if( portNumber > 0)
        {
            for( int ntries = 1; ; ntries++)
            {
                try
                {
                    Thread.sleep(1000); // Give the server more time to start
                }
                catch( InterruptedException ie) {}
                try
                {
                    drdaConn = DriverManager.getConnection(
														   TestUtil.getJdbcUrlPrefix(hostName,portNumber) + databaseName,
														   authenticationProperties);
                    break;
                }
                catch( SQLException sqle)
                {
                    if( ntries > 5)
                    {
                        passed = false;
                        System.out.println( "  Could not access database through the network server.");
                        System.out.println( "    " + sqle.getMessage());
                        return false;
                    }
                }
            }
        }
        return true;
    } // end of startTest

    private static boolean writeDerbyProperties( String[] properties)
    {
        derbyPropertiesFile.delete();
        try
        {
            derbyPropertiesFile.createNewFile();
            PrintStream propertyWriter = new PrintStream( new FileOutputStream( derbyPropertiesFile));
            propertyWriter.print( basePropertiesSB.toString());
            for( int i = 0; i < properties.length - 1; i += 2)
                propertyWriter.println( properties[i] + "=" + properties[i + 1]);

            propertyWriter.close();
            return true;
        }
        catch( IOException ioe)
        {
            passed = false;
            System.out.println( "  Could not create derby.properties: " + ioe.getMessage());
            return false;
        }
    } // end of writeDerbyProperties

    private static void deleteDir( String dirName)
    {
        deleteDir( new File( dirName));
    }

    private static void deleteDir( File parent)
    {
        if( ! parent.exists())
            return;
        if( parent.isDirectory())
        {
            String[] child = parent.list();
            for( int i = 0; i < child.length; i++)
                deleteDir( new File( parent, child[i]));
        }
        parent.delete();
    }

    private static void announceTest()
    {
        testNumber++;
        System.out.println( "Starting test case " + testNumber + ".");
        databaseName = "database" + testNumber;
    }
        
    private static void endTest( boolean autoStarted)
    {
        try
        {
            if( drdaConn != null)
            {
                drdaConn.close();
                drdaConn = null;
            }
            if( embeddedConn != null)
            {
                embeddedConn.close();
                embeddedConn = null;
            }
        }
        catch( SQLException sqle)
        {
            passed = false;
            System.out.println( "  Connection close failed:");
            System.out.println( "    " + sqle.getMessage());
        }
        // DERBY-803: Give the server threads time to finish their close
        // operations before we shut down the engine. Otherwise, we might get
        // some (harmless) error messages printed to the console. See also
        // DERBY-1020.
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ex) {}
        try
        {
            DriverManager.getConnection( "jdbc:derby:;shutdown=true");
        }
        catch( SQLException sqle)
        {
            if( ! sqle.getSQLState().equals( "XJ015"))
            {
                passed = false;
                System.out.println( "  System shutdown failed:");
                System.out.println( "    " + sqle.getMessage());
            }
        }
        serverOutputOut.flush();
        if( serverOutputBOS.size() > 0)
        {
            passed = false;
            System.out.println( "The auto-started server wrote to System.out.");
        }
        serverOutputBOS.reset();
        if( autoStarted && databaseName != null)
        {
            try
            {
                // Give the server thread time to shutdown, then check that it has done so.
                try
                {
                    Thread.sleep( 500);
                }
                catch( InterruptedException ie){};
                drdaConn = DriverManager.getConnection(
													   TestUtil.getJdbcUrlPrefix(hostName, portNumber) +  databaseName,
                                                        authenticationProperties);
                passed = false;
                System.out.println( "Was able to connect to the network server after Derby shut down.");
                drdaConn.close();
                drdaConn = null;
            }
            catch( SQLException sqle){};
        }
    } // end of endTest

	private static boolean isServerStarted(NetworkServerControl server)
	{
		try {
			server.ping();
		}
		catch (Exception e) {
			return false;
		}
		return true;
	}
}
