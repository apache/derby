/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ErrorCodeTest
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

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.LocaleTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Locale;

import org.apache.derbyTesting.junit.JDBC;

public final class ErrorCodeTest extends BaseJDBCTestCase {

    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public ErrorCodeTest(String name)
    {
        super(name);
    }

    public static Test suite()
    {
        TestSuite suite = new TestSuite("errorcode Test");
        
        suite.addTest(TestConfiguration.defaultSuite(ErrorCodeTest.class));
        
        return new LocaleTestSetup(suite, Locale.ENGLISH);
    }

    public void test_errorcode() throws Exception
    {
        ResultSet rs = null;
        
        Statement s = createStatement();
        
        s.executeUpdate(
            "create table t(i int, s smallint)");
        s.executeUpdate(
            "insert into t values (1,2)");
        s.executeUpdate("insert into t values (1,2)");
        s.executeUpdate("insert into t values (null,2)");
        
        //-- parser error
        //-- bug 5701        
        assertStatementError("42X94",30000,s,"create table t(i nt, s smallint)");
        
        //-- non-boolean where clause
        assertStatementError("42X19", 30000, s, "select * from t where i");
        
        // -- invalid correlation name for "*"
        assertStatementError("42X10",30000, s, "select asdf.* from t");
        
        //-- execution time error
        assertStatementError("22012",30000,s,"select i/0 from t");
        
        
       // -- test ErrorMessages VTI        
        rs = s.executeQuery(
            "select * from SYSCS_DIAG.error_Messages  where "
            + "CAST(sql_state AS CHAR(5)) = '07000'");
         
        String [][] expRS = new String [][]
        {
            {"07000", "At least one parameter to the current statement "
                + "is uninitialized.", "20000"}
        };        
        JDBC.assertFullResultSet(rs,expRS);
        // Test severe error messages. Existing messages should not change SQLState.
        // new ones can be added.
        rs = s.executeQuery("select * from SYSCS_DIAG.Error_messages where SEVERITY >= 40000 order by SQL_STATE");
        //Utilities.showResultSet(rs);
        String [][] expectedRows =
        {{"08000","Connection closed by unknown interrupt.","40000"},
        		{"08001","A connection could not be established because the security token is larger than the maximum allowed by the network protocol.","40000"},
        		{"08001","A connection could not be established because the user id has a length of zero or is larger than the maximum allowed by the network protocol.","40000"},
        		{"08001","A connection could not be established because the password has a length of zero or is larger than the maximum allowed by the network protocol.","40000"},
        		{"08001","A connection could not be established because the external name (EXTNAM) has a length of zero or is larger than the maximum allowed by the network protocol.","40000"},
        		{"08001","A connection could not be established because the server name (SRVNAM) has a length of zero or is larger than the maximum allowed by the network protocol.","40000"},
        		{"08001","Required Derby DataSource property {0} not set.","40000"},
        		{"08001","{0} : Error connecting to server {1} on port {2} with message {3}.","40000"},
        		{"08001","SocketException: '{0}'","40000"},
        		{"08001","Unable to open stream on socket: '{0}'.","40000"},
        		{"08001","User id length ({0}) is outside the range of 1 to {1}.","40000"},
        		{"08001","Password length ({0}) is outside the range of 1 to {1}.","40000"},
        		{"08001","User id can not be null.","40000"},
        		{"08001","Password can not be null.","40000"},
        		{"08001","A connection could not be established because the database name '{0}' is larger than the maximum length allowed by the network protocol.","40000"},
        		{"08003","No current connection.","40000"},
        		{"08003","getConnection() is not valid on a closed PooledConnection.","40000"},
        		{"08003","Lob method called after connection was closed","40000"},
        		{"08003","The underlying physical connection is stale or closed.","40000"},
        		{"08004","Connection refused : {0}","40000"},
        		{"08004","Connection authentication failure occurred.  Reason: {0}.","40000"},
        		{"08004","The connection was refused because the database {0} was not found.","40000"},
        		{"08004","Database connection refused.","40000"},
        		{"08004","User '{0}' cannot shut down database '{1}'. Only the database owner can perform this operation.","40000"},
        		{"08004","User '{0}' cannot (re)encrypt database '{1}'. Only the database owner can perform this operation.","40000"},
        		{"08004","User '{0}' cannot upgrade database '{1}'. Only the database owner can perform this operation.","40000"},
        		{"08004","Connection refused to database '{0}' because it is in replication slave mode.","40000"},
        		{"08004","User '{0}' cannot issue a replication operation on database '{1}'. Only the database owner can perform this operation.","40000"},
        		{"08004","Missing permission for user '{0}' to shutdown system [{1}].","40000"},
        		{"08004","Cannot check system permission to create database '{0}' [{1}].","40000"},
        		{"08004","Missing permission for user '{0}' to create database '{1}' [{2}].","40000"},
        		{"08004","Connection authentication failure occurred. Either the supplied credentials were invalid, or the database uses a password encryption scheme not compatible with the strong password substitution security mechanism. If this error started after upgrade, refer to the release note for DERBY-4483 for options.","40000"},
        		{"08004","Username or password is null or 0 length.","40000"},
                {"08004","User '{0}' cannot decrypt database '{1}'. Only the database owner can perform this operation.","40000"},
        		{"08006","An error occurred during connect reset and the connection has been terminated.  See chained exceptions for details.","40000"},
        		{"08006","SocketException: '{0}'","40000"},
        		{"08006","A communications error has been detected: {0}.","40000"},
        		{"08006","An error occurred during a deferred connect reset and the connection has been terminated.  See chained exceptions for details.","40000"},
        		{"08006","Insufficient data while reading from the network - expected a minimum of {0} bytes and received only {1} bytes.  The connection has been terminated.","40000"},
        		{"08006","Attempt to fully materialize lob data that is too large for the JVM.  The connection has been terminated.","40000"},
        		{"08006","A network protocol error was encountered and the connection has been terminated: {0}","40000"},
        		{"08006","org.apache.derby.jdbc.EmbeddedDriver is not registered with the JDBC driver manager","40000"},
        		{"08006","Database '{0}' shutdown.","45000"},
                {"08006","Database '{0}' dropped.","45000"},
        		{"0A000","The DRDA command {0} is not currently implemented.  The connection has been terminated.","40000"},
        		{"57017","There is no available conversion for the source code page, {0}, to the target code page, {1}.  The connection has been terminated.","40000"},
        		{"58009","Network protocol exception: only one of the VCM, VCS length can be greater than 0.  The connection has been terminated.","40000"},
        		{"58009","The connection was terminated because the encoding is not supported.","40000"},
        		{"58009","Network protocol exception: actual code point, {0}, does not match expected code point, {1}.  The connection has been terminated.","40000"},
        		{"58009","Network protocol exception: DDM collection contains less than 4 bytes of data.  The connection has been terminated.","40000"},
        		{"58009","Network protocol exception: collection stack not empty at end of same id chain parse.  The connection has been terminated.","40000"},
        		{"58009","Network protocol exception: DSS length not 0 at end of same id chain parse.  The connection has been terminated.","40000"},
        		{"58009","Network protocol exception: DSS chained with same id at end of same id chain parse.  The connection has been terminated.","40000"},
        		{"58009","Network protocol exception: end of stream prematurely reached while reading InputStream, parameter #{0}.  The connection has been terminated.","40000"},
        		{"58009","Network protocol exception: invalid FDOCA LID.  The connection has been terminated.","40000"},
        		{"58009","Network protocol exception: SECTKN was not returned.  The connection has been terminated.","40000"},
        		{"58009","Network protocol exception: only one of NVCM, NVCS can be non-null.  The connection has been terminated.","40000"},
        		{"58009","Network protocol exception: SCLDTA length, {0}, is invalid for RDBNAM.  The connection has been terminated.","40000"},
        		{"58009","Network protocol exception: SCLDTA length, {0}, is invalid for RDBCOLID.  The connection has been terminated.","40000"},
        		{"58009","Network protocol exception: SCLDTA length, {0}, is invalid for PKGID.  The connection has been terminated.","40000"},
        		{"58009","Network protocol exception: PKGNAMCSN length, {0}, is invalid at SQLAM {1}.  The connection has been terminated.","40000"},
        		{"58010","A network protocol error was encountered.  A connection could not be established because the manager {0} at level {1} is not supported by the server. ","40000"},
        		{"58014","The DDM command 0x{0} is not supported.  The connection has been terminated.","40000"},
        		{"58015","The DDM object 0x{0} is not supported.  The connection has been terminated.","40000"},
        		{"58016","The DDM parameter 0x{0} is not supported.  The connection has been terminated.","40000"},
        		{"58017","The DDM parameter value 0x{0} is not supported.  An input host variable may not be within the range the server supports.  The connection has been terminated.","40000"},
        		{"XBDA0","Login timeout exceeded.","40000"},
        		{"XBM01","Startup failed due to an exception. See next exception for details. ","45000"},
        		{"XBM02","Startup failed due to missing functionality for {0}. Please ensure your classpath includes the correct Derby software.","45000"},
        		{"XBM03","Supplied value '{0}' for collation attribute is invalid, expecting UCS_BASIC or TERRITORY_BASED.","45000"},
        		{"XBM04","Collator support not available from the JVM for the database's locale '{0}'.","45000"},
        		{"XBM05","Startup failed due to missing product version information for {0}.","45000"},
        		{"XBM06","Startup failed. An encrypted database cannot be accessed without the correct boot password.  ","45000"},
        		{"XBM07","Startup failed. Boot password must be at least 8 bytes long.","45000"},
        		{"XBM08","Could not instantiate {0} StorageFactory class {1}.","45000"},
        		{"XBM0A","The database directory '{0}' exists. However, it does not contain the expected '{1}' file. Perhaps Derby was brought down in the middle of creating this database. You may want to delete this directory and try creating the database again.","45000"},
			{"XBM0B","Failed to edit/write service properties file: {0}","45000"},
			{"XBM0C","Missing privilege for operation '{0}' on file '{1}': {2}", "45000"},
        		{"XBM0G","Failed to start encryption engine. Please make sure you are running Java 2 and have downloaded an encryption provider such as jce and put it in your class path. ","45000"},
        		{"XBM0H","Directory {0} cannot be created.","45000"},
        		{"XBM0I","Directory {0} cannot be removed.","45000"},
        		{"XBM0J","Directory {0} already exists.","45000"},
        		{"XBM0K","Unknown sub-protocol for database name {0}.","45000"},
        		{"XBM0L","Specified authentication scheme class {0} does implement the authentication interface {1}.","45000"},
        		{"XBM0M","Error creating an instance of a class named '{0}'. This class name was the value of the derby.authentication.provider property and was expected to be the name of an application-supplied implementation of org.apache.derby.authentication.UserAuthenticator. The underlying problem was: {1}","45000"},
        		{"XBM0N","JDBC Driver registration with java.sql.DriverManager failed. See next exception for details. ","45000"},
        		{"XBM0P","Service provider is read-only. Operation not permitted. ","45000"},
        		{"XBM0Q","File {0} not found. Please make sure that backup copy is the correct one and it is not corrupted.","45000"},
        		{"XBM0R","Unable to remove File {0}.  ","45000"},
        		{"XBM0S","Unable to rename file '{0}' to '{1}'","45000"},
        		{"XBM0T","Ambiguous sub-protocol for database name {0}.   ","45000"},
        		{"XBM0X","Supplied locale description '{0}' is invalid, expecting ln[_CO[_variant]]\nln=lower-case two-letter ISO-639 language code, CO=upper-case two-letter ISO-3166 country codes, see java.util.Locale.","45000"},
        		{"XBM0Y","Backup database directory {0} not found. Please make sure that the specified backup path is right.","45000"},
        		{"XBM0Z","Unable to copy file '{0}' to '{1}'. Please make sure that there is enough space and permissions are correct. ","45000"},
        		{"XCW00","Unsupported upgrade from '{0}' to '{1}'.","45000"},
        		{"XJ004","Database '{0}' not found.","40000"},
        		{"XJ015","Derby system shutdown.","50000"},
        		{"XJ028","The URL '{0}' is not properly formed.","40000"},
        		{"XJ040","Failed to start database '{0}' with class loader {1}, see the next exception for details.","40000"},
        		{"XJ041","Failed to create database '{0}', see the next exception for details.","40000"},
                {"XJ048","Conflicting boot attributes specified: {0}","40000"},
        		{"XJ049","Conflicting create attributes specified.","40000"},
        		{"XJ05B","JDBC attribute '{0}' has an invalid value '{1}', valid values are '{2}'.","40000"},
        		{"XJ081","Conflicting create/restore/recovery attributes specified.","40000"},
        		{"XJ213","The traceLevel connection property does not have a valid format for a number.","40000"},
        		{"XN022","A write chain that has transmitted data to the server cannot be reset until the request is finished and the chain terminated.","40000"},
        		{"XN023","The stream specified by parameter #{0} is locator-based and requires a nested request on the same connection to be materialized. This is not supported.","40000"},
        		{"XN024","Encountered an exception which terminated the connection, while reading from the stream specified by parameter #{0}. The Exception had this message: '{1}'.","40000"},
        		{"XRE20","Failover performed successfully for database '{0}', the database has been shutdown.","45000"},
        		{"XSDB0","Unexpected exception on in-memory page {0}","45000"},
        		{"XSDB1","Unknown page format at page {0}","45000"},
        		{"XSDB2","Unknown container format at container {0} : {1}","45000"},
        		{"XSDB3","Container information cannot change once written: was {0}, now {1}","45000"},
        		{"XSDB4","Page {0} is at version {1}, the log file contains change version {2}, either there are log records of this page missing, or this page did not get written out to disk properly.","45000"},
        		{"XSDB5","Log has change record on page {0}, which is beyond the end of the container.","45000"},
        		{"XSDB6","Another instance of Derby may have already booted the database {0}.","45000"},
        		{"XSDB7","Warning: Derby (instance {0}) is attempting to boot the database {1} even though Derby (instance {2}) may still be active.  Only one instance of Derby should boot a database at a time. Severe and non-recoverable corruption can result and may have already occurred.","45000"},
        		{"XSDB8","Warning: Derby (instance {0}) is attempting to boot the database {1} even though Derby (instance {2}) may still be active.  Only one instance of Derby should boot a database at a time. Severe and non-recoverable corruption can result if 2 instances of Derby boot on the same database at the same time.  The derby.database.forceDatabaseLock=true property has been set, so the database will not boot until the db.lck is no longer present.  Normally this file is removed when the first instance of Derby to boot on the database exits, but it may be left behind in some shutdowns.  It will be necessary to remove the file by hand in that case.  It is important to verify that no other VM is accessing the database before deleting the db.lck file by hand.","45000"},
        		{"XSDB9","Stream container {0} is corrupt.","45000"},
        		{"XSDBA","Attempt to allocate object {0} failed.","45000"},
        		{"XSDBB", "Unknown page format at page {0}, page dump follows: {1} ", "45000"},
        		{"XSDBC", "Write of container information to page 0 of container {0} failed.  See nested error for more information. ", "45000"},
        		{"XSDG0","Page {0} could not be read from disk.","45000"},
        		{"XSDG1","Page {0} could not be written to disk, please check if the disk is full, or if a file system limit, such as a quota or a maximum file size, has been reached.","45000"},
        		{"XSDG2","Invalid checksum on Page {0}, expected={1}, on-disk version={2}, page dump follows: {3}","45000"},
        		{"XSDG3","Meta-data for {0} could not be accessed to {1} {2}","45000"},
                {"XSDG4","Unrecoverable internal error encountered while attempting to read low level metadata about the table or index. Please provide your support organization with the following exception information: Failed: arraycopy of embryonic page byte[{0}] to container information byte[{1}]. Values of variables and constants: MAX_BORROWED_SPACE: {2}, BORROWED_SPACE_OFFSET({3}) + BORROWED_SPACE_LENG({4}) = {5}; arraycopylength: {6}; embryonic page {7}.","45000"},
        		{"XSDG5","Database is not in create mode when createFinished is called.","45000"},
        		{"XSDG6","Data segment directory not found in {0} backup during restore. Please make sure that backup copy is the right one and it is not corrupted.","45000"},
        		{"XSDG7","Directory {0} could not be removed during restore. Please make sure that permissions are correct.","45000"},
        		{"XSDG8","Unable to copy directory '{0}' to '{1}' during restore. Please make sure that there is enough space and permissions are correct. ","45000"},
        		{"XSDG9","Derby thread received an interrupt during a disk I/O operation, please check your application for the source of the interrupt.","45000"},
        		{"XSLA0","Cannot flush the log file to disk {0}.","45000"},
        		{"XSLA1","Log Record has been sent to the stream, but it cannot be applied to the store (Object {0}).  This may cause recovery problems also.","45000"},
        		{"XSLA2","System will shutdown, got I/O Exception while accessing log file.","45000"},
        		{"XSLA3","Log Corrupted, has invalid data in the log stream.","45000"},
        		{"XSLA4","Error encountered when attempting to write the transaction recovery log. Most likely the disk holding the recovery log is full. If the disk is full, the only way to proceed is to free up space on the disk by either expanding it or deleting files not related to Derby. It is also possible that the file system and/or disk where the Derby transaction log resides is read-only. The error can also be encountered if the disk or file system has failed.","45000"},
        		{"XSLA5","Cannot read log stream for some reason to rollback transaction {0}.","45000"},
        		{"XSLA6","Cannot recover the database.","45000"},
        		{"XSLA7","Cannot redo operation {0} in the log.","45000"},
        		{"XSLA8","Cannot rollback transaction {0}, trying to compensate {1} operation with {2}","45000"},
        		{"XSLAA","The store has been marked for shutdown by an earlier exception.","45000"},
        		{"XSLAB","Cannot find log file {0}, please make sure your logDevice property is properly set with the correct path separator for your platform.","45000"},
        		{"XSLAC","Database at {0} has a format incompatible with the current version of software. It may have been created by or upgraded by a later version.","45000"},
        		{"XSLAD","Log Record at instant {2} in log file {3} corrupted. Expected log record length {0}, real length {1}.","45000"},
        		{"XSLAE","Control file at {0} cannot be written or updated.","45000"},
        		{"XSLAF","A Read Only database was created with dirty data buffers.","45000"},
        		{"XSLAH","A Read Only database is being updated.","45000"},
        		{"XSLAI","Cannot log the checkpoint log record","45000"},
        		{"XSLAJ","The logging system has been marked to shut down due to an earlier problem and will not allow any more operations until the system shuts down and restarts.","45000"},
        		{"XSLAK","Database has exceeded largest log file number {0}.","45000"},
        		{"XSLAL","Log record size {2} exceeded the maximum allowable log file size {3}. Error encountered in log file {0}, position {1}.","45000"},
        		{"XSLAM","Cannot verify database format at {1} due to IOException: {0}","45000"},
        		{"XSLAN","Database at {0} has an incompatible format with the current version of the software.  The database was created by or upgraded by version {1}.","45000"},
        		{"XSLAO","Recovery failed unexpected problem: {0}.","45000"},
        		{"XSLAP","Database at {0} is at version {1}. Beta databases cannot be upgraded.","45000"},
        		{"XSLAQ","Cannot create log file at directory {0}.","45000"},
        		{"XSLAR","Unable to copy log file '{0}' to '{1}' during restore. Please make sure that there is enough space and permissions are correct. ","45000"},
        		{"XSLAS","Log directory {0} not found in backup during restore. Please make sure that backup copy is the correct one and it is not corrupted.","45000"},
        		{"XSLAT","The log directory '{0}' exists. The directory might belong to another database. Check that the location specified for the logDevice attribute is correct.","45000"},
        		{"XSTB0","An exception was thrown during transaction abort.","50000"},
        		{"XSTB2","Cannot log transaction changes, maybe trying to write to a read only database.","50000"},
        		{"XSTB3","Cannot abort transaction because the log manager is null, probably due to an earlier error.","50000"},
        		{"XSTB5","Creating database with logging disabled encountered unexpected problem.","50000"},
        		{"XSTB6","Cannot substitute a transaction table with another while one is already in use.","50000"},
                {"XXXXX","Normal database session close.","40000"},
                {"XRE04","Could not establish a connection to the peer of the replicated database '{0}' on address '{1}:{2}'.","40000"},
                {"XRE04","Connection lost for replicated database '{0}'.","40000"},
                {"XRE05","The log files on the master and slave are not in synch for replicated database '{0}'. The master log instant is {1}:{2}, whereas the slave log instant is {3}:{4}. This is fatal for replication - replication will be stopped.","40000"},
                {"XRE09","Cannot start replication slave mode for database '{0}'. The database has already been booted.","40000"},
                {"XRE11","Could not perform operation '{0}' because the database '{1}' has not been booted.","40000"},
                {"XRE21","Error occurred while performing failover for database '{0}', Failover attempt was aborted.","40000"},
                {"XRE22","Replication master has already been booted for database '{0}'","40000"},
                {"XRE41","Replication operation 'failover' or 'stopSlave' refused on the slave database because the connection with the master is working. Issue the 'failover' or 'stopMaster' operation on the master database instead.","40000"},
                {"XRE42","Replicated database '{0}' shutdown.","40000"}};

        JDBC.assertUnorderedResultSet(rs, expectedRows);
        s.executeUpdate("drop table t");
        commit();        
        s.close();
    }
}
