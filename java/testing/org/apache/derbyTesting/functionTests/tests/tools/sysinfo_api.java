/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.tools.sysinfo_api

   Copyright 1999, 2005 The Apache Software Foundation or its licensors, as appl
icable.

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

package org.apache.derbyTesting.functionTests.tests.tools;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import org.apache.derby.tools.sysinfo;
import org.apache.derbyTesting.functionTests.util.BaseJDBCTestCase;

/**
 *  Test all the static public methods of the sysinfo class.
 */

public class sysinfo_api extends BaseJDBCTestCase {

    Connection c;
    DatabaseMetaData dm;

    public sysinfo_api(String name) { 
        super(name); 
    }

    /*
     *  getMajorVersion()
     */
    public void testMajorVersion() {
        int dmMajor = dm.getDriverMajorVersion();
        assertEquals(dmMajor, sysinfo.getMajorVersion());
        assertEquals(dmMajor, sysinfo.getMajorVersion(sysinfo.DBMS));
        assertEquals(dmMajor, sysinfo.getMajorVersion(sysinfo.TOOLS));
        assertEquals(dmMajor, sysinfo.getMajorVersion(sysinfo.NET));
        assertEquals(dmMajor, sysinfo.getMajorVersion(sysinfo.CLIENT));
        // bad usage
        assertEquals(-1, sysinfo.getMajorVersion("foo"));
        assertEquals(-1, sysinfo.getMajorVersion(null));
    }

    /*
     *  getMinorVersion()
     */
    public void testMinorVersion() {
        int dmMinor = dm.getDriverMinorVersion();
        assertEquals(dmMinor, sysinfo.getMinorVersion());
        assertEquals(dmMinor, sysinfo.getMinorVersion(sysinfo.DBMS));
        assertEquals(dmMinor, sysinfo.getMinorVersion(sysinfo.TOOLS));
        assertEquals(dmMinor, sysinfo.getMinorVersion(sysinfo.NET));
        assertEquals(dmMinor, sysinfo.getMinorVersion(sysinfo.CLIENT));
        // bad usage
        assertEquals(-1, sysinfo.getMinorVersion("foo"));
        assertEquals(-1, sysinfo.getMinorVersion(null));
    }

    /*
     *  getProductName()
     */
    public void testProductName() {
        assertEquals("Apache Derby", sysinfo.getProductName());
        assertEquals("Apache Derby", sysinfo.getProductName(sysinfo.DBMS));
        assertEquals("Apache Derby", sysinfo.getProductName(sysinfo.TOOLS));
        assertEquals("Apache Derby", sysinfo.getProductName(sysinfo.NET));
        assertEquals("Apache Derby", sysinfo.getProductName(sysinfo.CLIENT));
        // bad usage
        assertEquals("<no name found>", sysinfo.getProductName("foo"));
        assertEquals("<no name found>", sysinfo.getProductName(null));
    }

    /*
     *  getVersionString()
     */
    public void testVersionString() throws SQLException {
        String dmPv = dm.getDatabaseProductVersion();
        assertEquals(dmPv, sysinfo.getVersionString());
        assertEquals(dmPv, sysinfo.getVersionString(sysinfo.DBMS));
        assertEquals(dmPv, sysinfo.getVersionString(sysinfo.TOOLS));
        assertEquals(dmPv, sysinfo.getVersionString(sysinfo.NET));
        assertEquals(dmPv, sysinfo.getVersionString(sysinfo.CLIENT));
        // bad usage
        assertEquals("<no name found>", sysinfo.getVersionString("foo"));
        assertEquals("<no name found>", sysinfo.getVersionString(null));
    }

    /*
     * getBuildNumber()
     *
     * Currently no test for sysinfo.getBuildNumber().
     * There is not currently a way to get this information from another
     * different public interface.
     */

    /*
     * getInfo()
     *
     * Currently only tests getInfo() by comparing the first line with the
     * expected first line in English. Because so much of sysinfo changes from
     * machine-to-machine, writing a better test may be difficult.
     *
     * Test spawns a separate thread in which to call sysinfo and feed the
     * PipedWriter. Using PipedWriter and PipedReader from the same thread
     * can cause a deadlock.
     */
    public void testGetInfo() throws IOException {
        sysinfo_api_helper sah = new sysinfo_api_helper();
        sah.start();
        PipedReader pipeR = new PipedReader(sah.getPipedWriter());
        BufferedReader br = new BufferedReader(pipeR);
        assertEquals("------------------ Java Information ------------------",
                     br.readLine());
        br.close();
        pipeR.close();
    }

    /*
     *  testSetup - get a DatabaseMetadata object with which to compare info
     *              with sysinfo
     */
    public void setUp() throws SQLException {
        c = getConnection();
        dm = c.getMetaData();
    }

    public void tearDown() throws SQLException {
        c.close();
    }

}

class sysinfo_api_helper extends Thread { 
    
    private static PipedWriter pipeW = new PipedWriter();

    public void run() {
        PrintWriter pw = new PrintWriter(pipeW, true);
        sysinfo.getInfo(pw);
        try {
            pw.close();
            pipeW.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

   public PipedWriter getPipedWriter() {
       return pipeW;
   }
}
