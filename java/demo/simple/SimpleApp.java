/*

   Derby - Class SimpleApp

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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Properties;


/**
 * This sample program is a minimal JDBC application showing
 * JDBC access to Derby.
 *
 * Instructions for how to run this program are
 * given in <A HREF=example.html>example.html</A>.
 *
 * Derby applications can run against Derby running in an embedded
 * or a client/server framework. When Derby runs in an embedded framework,
 * the Derby application and Derby run in the same JVM. The application
 * starts up the Derby engine. When Derby runs in a client/server framework,
 * the application runs in a different JVM from Derby. The application only needs
 * to start the client driver, and the connectivity framework provides network connections.
 * (The server must already be running.)
 *
 * <p>When you run this application, give one of the following arguments:
 *    * embedded (default, if none specified)
 *    * derbyclient (will use the Net client driver to access Network Server)
 *    * jccjdbcclient (if Derby is running embedded in the JCC Server framework)
 *
 */
public class SimpleApp
{
    /* the default framework is embedded*/
    public String framework = "embedded";
    public String driver = "org.apache.derby.jdbc.EmbeddedDriver";
    public String protocol = "jdbc:derby:";
    
    public String username = "user1";
    public String password = "user1";

    public static void main(String[] args)
    {
        new SimpleApp().go(args);
    }

    void go(String[] args)
    {
        /* parse the arguments to determine which framework is desired*/
        parseArguments(args);

        /* check for J2ME specification - J2ME must use a DataSource further on */
        String javaspec = System.getProperty( "java.specification.name" );
        boolean java2me = false;
        if( javaspec.indexOf( "J2ME" ) > -1 )
        {
            java2me = true;
        }
        
        System.out.println("SimpleApp starting in " + framework + " mode.");

        try
        {
            /*
               The driver is installed by loading its class.
               In an embedded environment, this will start up Derby, since it is not already running.
             */
            org.apache.derby.jdbc.EmbeddedSimpleDataSource ds = null;
            Connection conn = null;
            Properties props = new Properties();
            props.put("user", username);
            props.put("password", password);

            /* If we are using a J2ME jvm, we need to use a DataSource, otherwise
             * we can use java.sql.DriverManager to get the connection, or
             * a Datasource. This example program uses a DataSource with J2ME
             * but uses DriverManager otherwise.
             * If we were to use a DataSource for J2SE, we could use
             * the org.apache.derby.jdbc.EmbeddedDataSource, rather than the
             * org.apache.derby.jdbc.EmbeddedSimpleDataSource we need to use for J2ME.
             */
         
            if( java2me )
            {
                /*
                   The connection specifies create in the DataSource settings for
                   the database to be created. To remove the database,
                   remove the directory derbyDB and its contents.
                   The directory derbyDB will be created under
                   the directory that the system property
                   derby.system.home points to, or the current
                   directory if derby.system.home is not set.
                 */
       
                ds = new org.apache.derby.jdbc.EmbeddedSimpleDataSource();
                ds.setDatabaseName("derbyDB");
                ds.setCreateDatabase("create");
                conn = ds.getConnection(username, password);
            }
            else
            {
                /*
                   The connection specifies create=true in the url to cause
                   the database to be created. To remove the database,
                   remove the directory derbyDB and its contents.
                   The directory derbyDB will be created under
                   the directory that the system property
                   derby.system.home points to, or the current
                   directory if derby.system.home is not set.
                 */
          
                Class.forName(driver).newInstance();
                System.out.println("Loaded the appropriate driver.");
            
                conn = DriverManager.getConnection(protocol +
                    "derbyDB;create=true", props);
            }
            System.out.println("Connected to and created database derbyDB");

            conn.setAutoCommit(false);

            /*
               Creating a statement lets us issue commands against
               the connection.
             */
            Statement s = conn.createStatement();

            /*
               We create a table, add a few rows, and update one.
             */
            s.execute("create table derbyDB(num int, addr varchar(40))");
            System.out.println("Created table derbyDB");
            s.execute("insert into derbyDB values (1956,'Webster St.')");
            System.out.println("Inserted 1956 Webster");
            s.execute("insert into derbyDB values (1910,'Union St.')");
            System.out.println("Inserted 1910 Union");
            s.execute(
                "update derbyDB set num=180, addr='Grand Ave.' where num=1956");
            System.out.println("Updated 1956 Webster to 180 Grand");

            s.execute(
                "update derbyDB set num=300, addr='Lakeshore Ave.' where num=180");
            System.out.println("Updated 180 Grand to 300 Lakeshore");

            /*
               We select the rows and verify the results.
             */
            ResultSet rs = s.executeQuery(
                    "SELECT num, addr FROM derbyDB ORDER BY num");

            if (!rs.next())
            {
                throw new Exception("Wrong number of rows");
            }

            if (rs.getInt(1) != 300)
            {
                throw new Exception("Wrong row returned");
            }

            if (!rs.next())
            {
                throw new Exception("Wrong number of rows");
            }

            if (rs.getInt(1) != 1910)
            {
                throw new Exception("Wrong row returned");
            }

            if (rs.next())
            {
                throw new Exception("Wrong number of rows");
            }

            System.out.println("Verified the rows");

            s.execute("drop table derbyDB");
            System.out.println("Dropped table derbyDB");

            /*
               We release the result and statement resources.
             */
            rs.close();
            s.close();
            System.out.println("Closed result set and statement");

            /*
               We end the transaction and the connection.
             */
            conn.commit();
            conn.close();
            System.out.println("Committed transaction and closed connection");

            /*
               In embedded mode, an application should shut down Derby.
               If the application fails to shut down Derby explicitly,
               the Derby does not perform a checkpoint when the JVM shuts down, which means
               that the next connection will be slower.
               Explicitly shutting down Derby with the URL is preferred.
               This style of shutdown will always throw an "exception".
             */
            boolean gotSQLExc = false;

            if (framework.equals("embedded"))
            {
                /* again, with J2ME, we need to use a datasource to get the connection */
                if( java2me )
                {
                    try
                    {
                        ds.setShutdownDatabase( "shutdown" );
                        conn = ds.getConnection(username, password);
                    }
                    catch (SQLException se)
                    {
                        if( se.getErrorCode() == 45000 )
                        {
                            gotSQLExc = true;
                        }
                    }
                }
                else                  
                {   
                    try
                    {
                        DriverManager.getConnection("jdbc:derby:;shutdown=true");
                    }
                    catch (SQLException se)
                    {
                        gotSQLExc = true;
                    }
                }

                if (!gotSQLExc)
                {
                    System.out.println("Database did not shut down normally");
                }
                else
                {
                    System.out.println("Database shut down normally");
                }
            }
        }
        catch (Throwable e)
        {
            System.out.println("exception thrown:");

            if (e instanceof SQLException)
            {
                printSQLError((SQLException) e);
            }
            else
            {
                e.printStackTrace();
            }
        }

        System.out.println("SimpleApp finished");
    }

    static void printSQLError(SQLException e)
    {
        while (e != null)
        {
            System.out.println(e.toString());
            e = e.getNextException();
        }
    }

    private void parseArguments(String[] args)
    {
        int length = args.length;

        for (int index = 0; index < length; index++)
        {
            if (args[index].equalsIgnoreCase("jccjdbcclient"))
            {
                framework = "jccjdbc";
                driver = "com.ibm.db2.jcc.DB2Driver";
                protocol = "jdbc:derby:net://localhost:1527/";
            }
            if (args[index].equalsIgnoreCase("derbyclient"))
            {
                framework = "derbyclient";
                driver = "org.apache.derby.jdbc.ClientDriver";
                protocol = "jdbc:derby://localhost:1527/";
            }
        }
    }
}