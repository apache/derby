/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.VetJigsawTest
 *  
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.derbyTesting.functionTests.tests.lang;

import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SecurityManagerSetup;

/**
 * Test to verify that jigsaw module rules are applied.
 */
public class VetJigsawTest extends BaseJDBCTestCase
{
    ////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ////////////////////////////////////////////////

    private static final String[] JAR_FILES =
    {
        "derby.jar",
        "derbyclient.jar",
        "derbynet.jar",
        "derbyoptionaltools.jar",
        "derbyrun.jar",
        "derbyshared.jar",
        "derbytools.jar",
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        "derbyTesting.jar",
    };

    ////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ////////////////////////////////////////////////

  	public VetJigsawTest(String name)
    {
		super(name);
	}

    ////////////////////////////////////////////////
    //
    // JUnit MACHINERY
    //
    ////////////////////////////////////////////////

	/**
	 * Returns the implemented tests.
	 * 
	 * @return An instance of <code>Test</code> with the implemented tests to
	 *         run.
	 */
	public static Test suite()
    {
        // no need to install a security manager. we're just
        // verifying the jar file contents.
        BaseTestSuite baseTest = new BaseTestSuite( VetJigsawTest.class, "VetJigsawTest" );
        Test        cleanDatabaseWrapper = new CleanDatabaseTestSetup( baseTest );
        Test        noSecurityWrapper = SecurityManagerSetup.noSecurityManager( cleanDatabaseWrapper );

        return noSecurityWrapper;
	}		

    ////////////////////////////////////////////////
    //
    // TESTS
    //
    ////////////////////////////////////////////////

    /**
     * Verify that jar files do not share packages.
     */
	public void test_jarContents() throws Exception
    {
        final String className = "org.apache.derby.impl.jdbc.EmbedConnection";
        URL derbyURL = SecurityManagerSetup.getURL(className);
        String derbyJarFileName = derbyURL.toURI().getPath();
        String jarFileDirectory = derbyJarFileName.substring(0, derbyJarFileName.indexOf("derby.jar"));

        String result = null;
        
        try (Connection conn = getConnection())
        {
            loadJarFileContents(conn, jarFileDirectory);
            result = vetContents();
        }

        if ((result != null) && (result.length() != 0))
        {
            fail("Jar files overlap!\n" + result);
        }
    }
    private void loadJarFileContents(Connection conn, String jarFileDirectory) throws Exception
    {
        goodStatement
            (
             conn,
             "create function zipFile(zipFileName varchar( 32672 ))\n" +
             "returns table\n" +
             "(\n" +
             "      name   varchar( 100 ),\n" +
             "      directory varchar( 32672 ),\n" +
             "      comment varchar( 100 ),\n" +
             "      crc varchar(100),\n" +
             "      size bigint,\n" +
             "      modification_time timestamp\n" +
             ")\n" +
             "language java parameter style derby_jdbc_result_set no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.ZipFileTableFunction.zipFile'\n"
             );
        goodStatement
            (
             conn,
             "create table zipPackages\n" +
             "(\n" +
             "  packageName varchar(32672),\n" +
             "  zipFileName varchar(32672),\n" +
             "  primary key (packageName, zipFileName)\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "create table zipClasses\n" +
             "(\n" +
             "  packageName varchar(32672),\n" +
             "  zipFileName varchar(32672),\n" +
             "  className varchar(32672),\n" +
             "  primary key (packageName, zipFileName, className)\n" +
             ")\n"
             );

        for (String jarFileName : JAR_FILES)
        {
            loadJarFile(conn, jarFileDirectory, jarFileName);
        }

    }
    private void loadJarFile(Connection conn, String jarFileDirectory, String jarFileName)
        throws Exception
    {
        String fullJarFileName = jarFileDirectory + jarFileName;
        goodStatement
            (
             conn,
             "insert into zipPackages\n" +
             "  select distinct directory, '" + fullJarFileName + "'\n" +
             "  from table(zipfile('" + fullJarFileName + "')) t\n"
             );
        goodStatement
            (
             conn,
             "insert into zipClasses\n" +
             "  select directory, '" + fullJarFileName + "', name\n" +
             "  from table(zipFile('" + fullJarFileName + "')) t\n" +
//IC see: https://issues.apache.org/jira/browse/DERBY-6934
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
             "  where name like '%.class' and name <> 'module-info.class'"
             );
    }
    private String vetContents() throws Exception
    {
        String packageCounts =
          "select packageName, count(packageName) instanceCount\n" +
          "from zipPackages\n" +
          "where packageName not like 'META-INF%'\n" +
          "group by packageName\n" +
          "having count(packageName) > 1\n";
        StringBuilder buffer = new StringBuilder();
        
        try (PreparedStatement ps = prepareStatement(packageCounts))
        {
            try (ResultSet rs = ps.executeQuery())
            {
                while (rs.next())
                {
                    String packageName = rs.getString(1);
                    examinePackage(buffer, packageName);
                }
            }
        }

        return buffer.toString();
    }
    private void examinePackage(StringBuilder buffer, String packageName) throws Exception
    {
 
        String packageContents =
          "select zipFileName, className\n" +
          "from zipClasses\n" +
          "where packageName = '" + packageName + "'\n" +
          "order by zipFileName, className\n";
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        StringBuffer localBuffer = new StringBuffer();
        int zipsWithClasses = 0;
          
        try (PreparedStatement ps = prepareStatement(packageContents))
        {
            String lastZipFileName = null;
            try (ResultSet rs = ps.executeQuery())
            {
                while(rs.next())
                {
                    String zipFileName = rs.getString(1);
                    String className = rs.getString(2);
                    if ((lastZipFileName != null) && !zipFileName.equals(lastZipFileName))
                    { zipsWithClasses++; }

                    localBuffer
                      .append("    ")
                      .append(zipFileName)
                      .append("\t")
                      .append(className)
                      .append("\n");
                }
            }
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        if (zipsWithClasses > 1)
        {
            buffer
              .append(packageName + " straddles more than one jar file:\n")
              .append(localBuffer.toString())
              .append("\n");
        }
    }

}
