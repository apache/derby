/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.UpgradeTrajectoryTest

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
package org.apache.derbyTesting.functionTests.tests.upgradeTests;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import javax.sql.DataSource;
import junit.extensions.TestSetup;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.ClassLoaderTestSetup;
import org.apache.derbyTesting.junit.JDBCClient;
import org.apache.derbyTesting.junit.JDBCClientSetup;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * <p>
 * Test upgrade trajectories. This test compares the metadata in
 * upgraded databases to the metadata in databases created from scratch.
 * Given a collection of releases, this test does the following:
 * </p>
 *
 * <ul>
 * <li>Builds either a minimal set of trajectories or the full set of possible
 * trajectories:
 *  <ul>
 *   <li>By default, just builds a minimal set of trajectories. These are all
 *   trajectories which begin with some release, then upgrade through all
 *   intermediate releases to the highest release in the original collection of
 *   all releases. A set of N releases gives rise to N-1 minimal trajectories.</li>
 *
 *   <li>Otherwise, if the system property {@code derbyTesting.allTrajectories}
 *   is set to true, builds the set of all upgrade trajectories possible on
 *   that collection of releases. An upgrade trajectory is a sorted subset of
 *   those releases. Each subset is sorted in ascending release order. We
 *   exclude the vacuous empty subset and the uninteresting singleton
 *   subsets. A set of N releases gives rise to ((2**N) - N) - 1 hard-upgrade
 *   trajectories.</li>
 *  </ul>
 * </li>
 * <li>For each trajectory, we create two databases:
 *  <ul>
 *   <li>A starting point database created with the first release in the
 *   trajectory. This database is then upgraded through all of the intermediate
 *   releases in the trajectory until it is at the level of the last release in
 *   the trajectory.</li>
 *   <li>An ending point database created with the last release in the
 *   trajectory.</li>
 *  </ul>
 *</li>
 * <li>We then compare the metadata in the starting point and ending point
 * databases.</li>
 * </ul>
 *
 * <p>
 * By default we don't consider soft-upgrades. Also by default, we consider
 * trajectories with more than one release from the same branch. You can
 * parameterize or customize some constants (see below) if you want to change
 * these decisions.
 * </p>
 *
 * <p>
 * By default we consider all  trajectories possible on the collection
 * of releases listed in {@link OldVersions}. If you want to consider
 * a different  collection of  releases, you  can override  the {@code
 * OldVersions}  collection  by  setting the  system  property  {@code
 * "derbyTesting.oldVersionsPath"}. Here, for instance, is the command
 * line to run this test against a customized list of releases:
 * </p>
 *
 * <blockquote><pre>
 *  java -XX:MaxPermSize=128M -Xmx512m \
 *  -DderbyTesting.oldReleasePath=/Users/me/myDerbyReleaseDirectory \
 *  -DderbyTesting.oldVersionsPath=/Users/me/fileContainingMyListOfTastyReleases \
 *  junit.textui.TestRunner org.apache.derbyTesting.functionTests.tests.upgradeTests.UpgradeTrajectoryTest
 * </pre></blockquote>
 *
 * <p>
 * For extra verbose output, you can set the "derby.tests.debug" property too:
 * </p>
 *
 * <blockquote><pre>
 *  java -XX:MaxPermSize=128M -Xmx512m \
 *  -DderbyTesting.oldReleasePath=/Users/me/myDerbyReleaseDirectory \
 *  -DderbyTesting.oldVersionsPath=/Users/me/fileContainingMyListOfTastyReleases \
 *  -Dderby.tests.debug=true \
 *  junit.textui.TestRunner org.apache.derbyTesting.functionTests.tests.upgradeTests.UpgradeTrajectoryTest
 * </pre></blockquote>
 *
 * <p>
 * Here is the command line to run all upgrade trajectories against a customized list of releases:
 * </p>
 *
 * <blockquote><pre>
 *  java -XX:MaxPermSize=128M -Xmx512m \
 *  -DderbyTesting.allTrajectories=true \
 *  -DderbyTesting.oldReleasePath=/Users/me/myDerbyReleaseDirectory \
 *  -DderbyTesting.oldVersionsPath=/Users/me/fileContainingMyListOfTastyReleases \
 *  junit.textui.TestRunner org.apache.derbyTesting.functionTests.tests.upgradeTests.UpgradeTrajectoryTest
 * </pre></blockquote>
 *
 * <p>
 * If you need to test a particular trajectory, you can hand-edit
 * {@code makeSampleTrajectories()} and uncomment the call to it.
 * </p>
 *
 */
public class UpgradeTrajectoryTest extends BaseJDBCTestCase
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public static final String ALL_TRAJECTORIES_PROPERTY = "derbyTesting.allTrajectories";

    public static Version VERSION_10_0_2_1 = new Version( 10, 0, 2, 1 );
    public static Version VERSION_10_1_3_1 = new Version( 10, 1, 3, 1 );
    public static Version VERSION_10_2_2_0 = new Version( 10, 2, 2, 0 );
    public static Version VERSION_10_5_1_1 = new Version( 10, 5, 1, 1 );
    public static Version VERSION_10_6_0_0 = new Version( 10, 6, 0, 0 );

    public static Version.Trajectory TRAJECTORY_10_0_2_1_TO_10_1_3_1 = new Version.Trajectory( new Version[] { VERSION_10_0_2_1, VERSION_10_1_3_1 } );

    public static String BRANCH_10_0 = "10.0";
    public static String BRANCH_10_1 = "10.1";
    public static String BRANCH_10_2 = "10.2";
    public static String BRANCH_10_3 = "10.3";
    public static String BRANCH_10_4 = "10.4";
    public static String BRANCH_10_5 = "10.5";

    public static final String UPGRADED_DATABASE = "old_database";
    public static final String VIRGIN_DATABASE = "new_database";
    public static final String COMPARISON_DATABASE = "comparison_database";
    private static final String DUMMY_NUMBER = "123";
    private static final String DUMMY_STRING = "BLAHBLAH";
    private static final String DUMMY_TIMESTAMP = "123456";

    private static final String DERBY_4214_1 = "RETURNS VARCHAR(32672)";
    private static final String DERBY_4214_2 = "RETURNS VARCHAR(10890)";
    private static final String DERBY_4215 = "SYSCS_INPLACE_COMPRESS_TABLE";

    //
    // Parameterize or change these switches if you want to alter the set of
    // trajectories which we consider:
    //
    private static final boolean TRJ_IGNORE_SOFT_UPGRADE = true;
    private static final boolean TRJ_SAME_BRANCH_NEIGHBORS = false;

    private static final String SYSALIASES = "SYSALIASES";
    private static final String SYSCONGLOMERATES = "SYSCONGLOMERATES";
    private static final String SYSSTATEMENTS = "SYSSTATEMENTS";
    private static final String SYSROUTINEPERMS = "SYSROUTINEPERMS";

    private static final String CONGLOMERATENUMBER = "CONGLOMERATENUMBER";
    private static final String ALIAS = "ALIAS";
    private static final String ALIASID = "ALIASID";
    private static final String SPECIFICNAME = "SPECIFICNAME";
    private static final String STMTID = "STMTID";
    private static final String STMTNAME = "STMTNAME";
    private static final String TEXT = "TEXT";
    private static final String ROUTINEPERMSID = "ROUTINEPERMSID";
    private static final String LASTCOMPILED = "LASTCOMPILED";

    private static final boolean LOQUACIOUS = false;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private Version.Trajectory _trajectory;
    private String             _trajectoryName;
    private boolean[]          _hardUpgradeRequests;
    
    private HashMap<String,String>     _unstableColumnValues = new HashMap<String,String>();

    private static ThreadLocal<ClassLoader> _originalClassLoader = new ThreadLocal<ClassLoader>();

    // these are the system tables which must be read first in order to
    // prep the mapping of unstable identifiers
    private static String[] INITIAL_TABLES = new String[]
    {
        "SYSALIASES",
    };


    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public UpgradeTrajectoryTest( Version.Trajectory trajectory, boolean[] hardUpgradeRequests )
    {
        super( "testTrajectory" );

        _trajectory = trajectory;
        _hardUpgradeRequests = hardUpgradeRequests;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Return the suite of tests.
     */
    public static Test suite()
    {
        BaseTestSuite suite = new BaseTestSuite("Upgrade trajectory test");
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        addTrajectories( suite );
        
        Test sfs = new SupportFilesSetup((Test) suite);

        return SecurityManagerSetup.noSecurityManager( sfs );
    }

    /**
     * <p>
     * Add all of the trajectories we intend to test.
     * </p>
     */
    private static void addTrajectories( BaseTestSuite suite )
    {
        Version.Trajectory[] trajectories = makeTrajectories();
        int                  count = trajectories.length;

        println( "Found " + count + " trajectories." );

        for ( int i = 0; i < count; i++ )
        {
            // right now, we're only testing hard upgrade.
            addHardUpgradeOnlyTrajectory( suite, trajectories[ i ] );
        }
    }
    
    /**
     * <p>
     * Make all known upgrade trajectories. This amounts to every non-vacuous
     * subset of the known versions.
     * </p>
     */
    private static Version.Trajectory[] makeTrajectories()
    {
        Version[]   supportedVersions = getSupportedVersions();
        Version.Trajectory[] result;

        //
        // If requested to, we construct the power set of supported versions, throwing
        // out the non-upgradable subsets, viz., the empty set and the
        // singletons. We include hard upgrades between releases on the same
        // branch.
        //
        // By default we don't test all trajectories. We only test the
        // trajectories which are complete sequences from a starting point up to
        // the last release.
        //
        if ( shouldBuildAllTrajectories() ) { result = buildPowerSet( supportedVersions ); }
        else { result = buildMinimalSet( supportedVersions ); }
        
        //
        // Uncomment this line if you just want to test a couple sample
        // trajectories.
        //
        //result = makeSampleTrajectories();
        
        return result;
    }
    
    /**
     * <p>
     * Return true if we should build all trajectories.
     * </p>
     */
    private static boolean shouldBuildAllTrajectories()
    {
        Boolean bool = Boolean.valueOf(getSystemProperty( ALL_TRAJECTORIES_PROPERTY ));
//IC see: https://issues.apache.org/jira/browse/DERBY-6856

        return bool.booleanValue();
    }
    
    /**
     * <p>
     * Sample trajectory for debugging this program.
     * </p>
     */
    private static Version.Trajectory[] makeSampleTrajectories()
    {
        return new Version.Trajectory[]
        {
            new Version.Trajectory( new Version[] { new Version( 10, 0, 2, 1), new Version( 10, 1, 3, 1 ) } ),
            new Version.Trajectory( new Version[] { new Version( 10, 0, 2, 1), new Version( 10, 3, 3, 0 ) } ),
            new Version.Trajectory( new Version[] { new Version( 10, 0, 2, 1), new Version( 10, 3, 3, 0 ), new Version( 10, 5, 1, 1 ) } ),
            new Version.Trajectory( new Version[] { new Version( 10, 0, 2, 1), new Version( 10, 3, 3, 0 ), new Version( 10, 6, 0, 0 ) } ),
            new Version.Trajectory( new Version[] { new Version( 10, 0, 2, 1), new Version( 10, 5, 1, 1 ) } ),
            new Version.Trajectory( new Version[] { new Version( 10, 4, 2, 1), new Version( 10, 5, 1, 1 ) } ),
        };

    }
    
    /**
     * <p>
     * Get the supported versions.
     * </p>
     */
    private static Version[] getSupportedVersions()
    {
        int[][]      raw = OldVersions.getSupportedVersions();
        int          count = raw.length;
        Version[] result = new Version[ count ];

        for ( int i = 0; i < count; i++ ) { result[ i ] = new Version( raw[ i ] ); }

        return result;
    }
    
    /**
     * <p>
     * Add only the test case which hard-upgrades along all edges of the Trajectory.
     * </p>
     */
    private static void addHardUpgradeOnlyTrajectory(
        BaseTestSuite suite, Version.Trajectory trajectory )
    {
        // a valid trajectory must have a start point and a different end point
        int       versionCount = trajectory.getVersionCount();
        if ( versionCount < 2 ) { return; }
        
        boolean[] hardUpgradeRequests = new boolean[ versionCount ];

        // the start point is always hard
        for ( int i = 0; i < versionCount; i++ ) { hardUpgradeRequests[ i ] = true; }

        addTrajectory( suite, trajectory, hardUpgradeRequests );
    }

    /**
     * <p>
     * Add a single trajectory to the suite, looping through all combinations of
     * hard and softupgrade.
     * </p>
     */
    private static void addTrajectory(
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite, Version.Trajectory trajectory )
    {
        // a valid trajectory must have a start point and a different end point
        int       versionCount = trajectory.getVersionCount();
        if ( versionCount < 2 ) { return; }
        
        boolean[] hardUpgradeRequests = new boolean[ versionCount ];

        // the start point is always hard unless you parameterize the following constant
        hardUpgradeRequests[ 0 ] = TRJ_IGNORE_SOFT_UPGRADE;

        addTrajectory( suite, trajectory, hardUpgradeRequests, 1 );
    }

    private static void addTrajectory(
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite,
        Version.Trajectory trajectory,
        boolean[] hardUpgradeRequests,
        int idx )
    {
        if ( idx >= trajectory.getVersionCount() )
        {
            addTrajectory( suite, trajectory, hardUpgradeRequests );
        }
        else
        {
            boolean[] hard = clone( hardUpgradeRequests );
            boolean[] soft = clone( hardUpgradeRequests );
            
            hard[ idx ] = true;
            addTrajectory( suite, trajectory, hard, idx + 1 );

            soft[ idx ] = false;
            addTrajectory( suite, trajectory, soft, idx + 1 );
        }
    }

    private static boolean[] clone( boolean[] input )
    {
        int       count = input.length;
        boolean[] output = new boolean[ count ];

        for ( int i = 0; i < count; i++ ) { output[ i ] = input[ i ]; }

        return output;
    }

    /**
     * <p>
     * Add a single trajectory to the suite, with upgrade instructions.
     * </p>
     */
    private static void addTrajectory(
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite,
        Version.Trajectory trajectory,
        boolean[] hardUpgradeRequests )
    {
        UpgradeTrajectoryTest utt = new UpgradeTrajectoryTest( trajectory, hardUpgradeRequests );
        TestSetup setup = TestConfiguration.additionalDatabaseDecorator( utt, UPGRADED_DATABASE );
        setup =  TestConfiguration.additionalDatabaseDecorator( setup, VIRGIN_DATABASE );
        setup =  TestConfiguration.additionalDatabaseDecorator( setup, COMPARISON_DATABASE );

        Properties preReleaseUpgrade = new Properties();
        preReleaseUpgrade.setProperty( "derby.database.allowPreReleaseUpgrade", "true");
        
        setup = new SystemPropertyTestSetup(setup, preReleaseUpgrade );

        // If the first release in the trajectory pre-dates the release which
        // introduced JDBC4, force the client to be the JDBC3 client. This
        // prevents us from falling through and picking up the JDBC4 data source from
        // the system classpath rather than picking up a datasource from
        // the version-specific classloader.
//IC see: https://issues.apache.org/jira/browse/DERBY-4359
        if ( trajectory.getVersion( 0 ).compareTo( VERSION_10_2_2_0 ) < 0 )
        {
            setup = new JDBCClientSetup( setup, JDBCClient.EMBEDDED_30 );
        }
        
        suite.addTest( setup );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test a single trajectory.
     * </p>
     */
    public void testTrajectory() throws Exception
    {
        _trajectoryName = _trajectory.toString() + ' ' + stringifyUpgradeRequests();

        println( "Testing trajectory: " + _trajectoryName );

        // there must be at least 2 versions in a trajectory, otherwise the
        // test is vacuous
        int  versionCount = _trajectory.getVersionCount();
        assertTrue( _trajectoryName, versionCount > 1 );

        // remember the original class loader
        saveOriginalClassLoader();

        try {
            Version startDataVersion = _trajectory.getVersion( 0 );
            Version endDataVersion = startDataVersion;

            createDatabase( startDataVersion, UPGRADED_DATABASE );

            // now upgrade the database through the whole chain
            for ( int i = 1; i < versionCount; i++ )
            {
                Version nextSoftwareVersion = _trajectory.getVersion( i );
                boolean hardUpgrade = _hardUpgradeRequests[ i ];

                if ( hardUpgrade ) { endDataVersion = nextSoftwareVersion; }

                upgradeDatabase( nextSoftwareVersion, endDataVersion, hardUpgrade, UPGRADED_DATABASE );
            }

            createDatabase( endDataVersion, VIRGIN_DATABASE );

            if ( LOQUACIOUS) { println( "    End version is " + endDataVersion ); }

            compareDatabases( endDataVersion, UPGRADED_DATABASE, VIRGIN_DATABASE );
        }
        finally
        {
            restoreOriginalClassLoader();

            // reboot the databases so that DropDatabaseSetup can remove them
            bootDatabase( UPGRADED_DATABASE );
            bootDatabase( VIRGIN_DATABASE );
        }
    }

    /**
     * <p>
     * Compare the metadata in two databases.
     * </p>
     */
    private void compareDatabases( Version version, String leftDatabaseName, String rightDatabaseName )
        throws Exception
    {
        ClassLoaderTestSetup.setThreadLoader( version.getClassLoader() );
//IC see: https://issues.apache.org/jira/browse/DERBY-6619
//IC see: https://issues.apache.org/jira/browse/DERBY-3745

        DataSource leftDS = makeDataSource( leftDatabaseName );
        DataSource rightDS = makeDataSource( rightDatabaseName );
        DataSource comparisonDS = bootDatabase( COMPARISON_DATABASE );
        Connection leftConn = leftDS.getConnection();
        Connection rightConn = rightDS.getConnection();
        Connection comparisonConn = comparisonDS.getConnection();

        //        compareQueries( leftConn, rightConn, 2, "select stmtname, lastcompiled from sys.sysroutineperms order by stmtname" );
        //        boolean b = true;
        //        if ( b ) { return; }


        try {

            goodStatement( comparisonConn, "create schema " + leftDatabaseName );
            goodStatement( comparisonConn, "create schema " + rightDatabaseName );

            compareResults
                ( leftConn, rightConn, comparisonConn, leftDatabaseName, rightDatabaseName, "first_table", "select tablename from sys.systables where tabletype = 'S'" );

            // compare the tables which must come first
            int initialTableCount = INITIAL_TABLES.length;
            for ( int i = 0; i < initialTableCount; i++ )
            {
                String systemTableName = INITIAL_TABLES[ i ];
                compareResults
                    ( leftConn, rightConn, comparisonConn, leftDatabaseName, rightDatabaseName, systemTableName, "select * from sys." + systemTableName );
            }

            // now compare the other tables
            ArrayList systemTables = listSystemTables( comparisonConn );
            int          count = systemTables.size();

            for ( int i = 0; i < count; i++ )
            {
                String systemTableName = (String) systemTables.get( i );
                compareResults
                    ( leftConn, rightConn, comparisonConn, leftDatabaseName, rightDatabaseName, systemTableName, "select * from sys." + systemTableName );
            }

        }
        finally
        {
            shutdownDatabase( leftDS );
            shutdownDatabase( rightDS );
            shutdownDatabase( comparisonDS );
        }

    }
    
    /**
     * <p>
     * Get the names of all the system tables in the database except for the
     * list of initial tables.
     * </p>
     */
    private ArrayList<String> listSystemTables
        ( Connection conn )
        throws Exception
    {
        ArrayList<String> result = new ArrayList<String>();
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

        StringBuffer buffer = new StringBuffer();

        buffer.append( "select tablename from sys.systables where tabletype = 'S' and tablename != 'SYSDUMMY1'" );
        int initialTableCount = INITIAL_TABLES.length;
        for ( int i = 0; i < initialTableCount; i++ )
        {
            buffer.append( " and tablename != '" );
            buffer.append( INITIAL_TABLES[ i ] );
            buffer.append( "'" );
        }
        buffer.append( " order by tablename" );
        
        PreparedStatement ps = chattyPrepare( conn,  buffer.toString() );
        ResultSet               rs = ps.executeQuery();

        while ( rs.next() )
        {
            result.add( rs.getString( 1 ) );
        }

        rs.close();
        ps.close();

        return result;
    }
    
    /**
     * <p>
     * Compare the results of a query in two databases.
     * </p>
     */
    private void compareResults
        ( Connection leftConn, Connection rightConn, Connection comparisonConn, String leftSchema, String rightSchema, String tableName, String query )
        throws Exception
    {
        String leftTableName = leftSchema + "." + tableName;
        String rightTableName = rightSchema + "." + tableName;
        StringBuffer columnList = new StringBuffer();
        StringBuffer insertList = new StringBuffer();
        ArrayList<String> columnNames = new ArrayList<String>();
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

        PreparedStatement leftPS = chattyPrepare( leftConn, query );
        PreparedStatement rightPS = chattyPrepare( rightConn, query );
        ResultSet leftSelect = leftPS.executeQuery();
        ResultSet rightSelect = rightPS.executeQuery();
        PreparedStatement leftInsert = null;
        PreparedStatement rightInsert = null;

        try {
            //
            // First create the tables to hold the left and right results.
            //
            ResultSetMetaData leftRSMD = leftSelect.getMetaData();
            ResultSetMetaData rightRSMD = rightSelect.getMetaData();
            int columnCount = leftRSMD.getColumnCount();
            String sampleColumnName = null;

            assertEquals( _trajectoryName, leftRSMD.getColumnCount(), rightRSMD.getColumnCount() );

            columnList.append( "\n( " );
            insertList.append( "\n( " );
            for ( int i = 1; i <= columnCount; i++ )
            {
                if ( i > 1 )
                {
                    columnList.append( ", " );
                    insertList.append( ", " );
                }
                String columnName = leftRSMD.getColumnName(  i );
                if ( i == 1 ) { sampleColumnName = columnName; }
                columnNames.add( columnName );
                
                assertEquals( _trajectoryName, leftRSMD.getColumnName( i ), rightRSMD.getColumnName( i ) );
                assertEquals( _trajectoryName, leftRSMD.getColumnType( i ), rightRSMD.getColumnType( i  ) );

                columnList.append( columnName );
                columnList.append( " varchar( 10000 )" );
                insertList.append( "?" );
            }
            columnList.append( "\n)" );
            insertList.append( "\n)" );

            String colList = columnList.toString();
            String insList = insertList.toString();

            goodStatement( comparisonConn, "create table " + leftTableName + colList );
            goodStatement( comparisonConn, "create table " + rightTableName + colList );
            
            leftInsert = chattyPrepare( comparisonConn, "insert into " + leftTableName + " values " + insList );
            rightInsert = chattyPrepare( comparisonConn, "insert into " + rightTableName + " values " + insList );

            // now loop through the metadata rows, copying them into the
            // comparison tables
            int leftCount = stuffTable( tableName, leftSelect, leftInsert );
            int rightCount = stuffTable( tableName, rightSelect, rightInsert );

            if (
                (!suffersDERBY_4215( tableName )) &&
                (!suffersDERBY_4216( tableName ))
                )
            { assertEquals( _trajectoryName + ": " + tableName, leftCount, rightCount ); }
            else { assertTrue( _trajectoryName + ": " + tableName, leftCount != rightCount ); }

            // now compare the copied metadata, using left joins
            leftJoin( comparisonConn, columnNames, tableName, leftTableName, rightTableName );
            if ( !suffersDERBY_4216( tableName ) )
            { leftJoin( comparisonConn, columnNames, tableName, rightTableName, leftTableName ); }
        }
        finally
        {
            leftSelect.close();
            rightSelect.close();
            leftPS.close();
            rightPS.close();

            if ( leftInsert != null ) { leftInsert.close(); }
            if ( rightInsert != null ) { rightInsert.close(); }
        }
        
    }

    /**
     * <p>
     * Copy metadata from a source table into a target table.
     * </p>
     */
    private int stuffTable( String tableName, ResultSet select, PreparedStatement insert ) throws Exception
    {
        ResultSetMetaData rsmd = select.getMetaData();
        int  columnCount = rsmd.getColumnCount();
        int  rowCount = 0;

        String[] columnNames = new String[ columnCount ];
        for ( int i = 0; i < columnCount; i++ ) { columnNames[ i ] = rsmd.getColumnName( i + 1 ); }

        String[] row = new String[ columnCount ];

        while ( select.next() )
        {
            for ( int i = 0; i < columnCount; i++ )
            {
                row[ i ] = select.getString( i + 1 );
            }
            normalizeRow( tableName, columnNames, row );
            
            for ( int i = 0; i < columnCount; i++ )
            {
                insert.setString( i + 1, row[ i ]  );
            }
            insert.executeUpdate();
            rowCount++;
        }

        return rowCount;
    }
    
    /**
     * <p>
     * Left join two tables and verify that the result is empty.
     * </p>
     */
    private void leftJoin( Connection conn, ArrayList columnNames, String tableName, String leftTableName, String rightTableName )
        throws Exception
    {
        PreparedStatement selectPS = null;
        ResultSet select = null;

        String sampleColumn = (String) columnNames.get( 0 );
        StringBuffer buffer = new StringBuffer();

        buffer.append( "select *\nfrom " );
        buffer.append( leftTableName );
        buffer.append( " l left join " );
        buffer.append( rightTableName );
        buffer.append( " r\non " );
        int count = columnNames.size();
        for ( int i = 0; i < count; i++ )
        {
            if ( i > 0 ) { buffer.append( "and " ); }

            String columnName = (String) columnNames.get( i );
            buffer.append( "( ( l." );
            buffer.append( columnName );
            buffer.append( " = r." );
            buffer.append( columnName );
            buffer.append( " ) or ( l." );
            buffer.append( columnName );
            buffer.append( " is null and r." );
            buffer.append( columnName );
            buffer.append( " is null ) )\n" );
        }
        buffer.append( "where r." );
        buffer.append( sampleColumn );
        buffer.append( " is null" );
        

        try {
            selectPS = chattyPrepare
                (
                 conn,
                 buffer.toString()
                 );
            select = selectPS.executeQuery();

            String expected = "";
            String actual = filterKnownProblems( tableName, printResultSet( select ) );

            assertEquals( _trajectoryName + ": " + leftTableName + " vs. " + rightTableName, expected, actual );
        }
        finally
        {
            if ( select != null ) { select.close(); }
            if ( selectPS != null ) { selectPS.close(); }
        }

    }
    
    /**
     * <p>
     * Create a database using the indicated version of Derby.
     * </p>
     */
    private void createDatabase( Version version, String logicalDatabaseName )
        throws Exception
    {
        ClassLoaderTestSetup.setThreadLoader( version.getClassLoader() );
//IC see: https://issues.apache.org/jira/browse/DERBY-6619
//IC see: https://issues.apache.org/jira/browse/DERBY-3745

        DataSource ds = bootDatabase( logicalDatabaseName );

        Connection conn = ds.getConnection();

        vetDBVersion( version, version, conn );

        shutdownDatabase( ds );
    }

    /**
     * <p>
     * Upgrade a database to the indicated version of Derby.
     * </p>
     */
    private void upgradeDatabase( Version softwareVersion, Version dataVersion, boolean hardUpgrade, String logicalDatabaseName )
        throws Exception
    {
        ClassLoaderTestSetup.setThreadLoader(softwareVersion.getClassLoader());
//IC see: https://issues.apache.org/jira/browse/DERBY-6619
//IC see: https://issues.apache.org/jira/browse/DERBY-3745

        DataSource ds = upgradeDatabase( logicalDatabaseName, hardUpgrade );

        Connection conn = ds.getConnection();

        vetDBVersion( softwareVersion, dataVersion, conn );

        shutdownDatabase( ds );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Boot a database.
     * </p>
     */
    private DataSource bootDatabase( String logicalDatabaseName )
        throws Exception
    {
        DataSource ds = makeDataSource( logicalDatabaseName );
        Method     setMethod = ds.getClass().getMethod( "setCreateDatabase", new Class[] { String.class } );
        
        setMethod.invoke( ds, new Object[] { "create" } );
        
        Connection conn = ds.getConnection();
        
        return ds;
    }
    
    /**
     * <p>
     * Hard-upgrade a database.
     * </p>
     */
    private DataSource upgradeDatabase( String logicalDatabaseName, boolean hardUpgrade )
        throws Exception
    {
        DataSource ds = makeDataSource( logicalDatabaseName );

        if ( hardUpgrade )
        {
            Method     setMethod = ds.getClass().getMethod( "setConnectionAttributes", new Class[] { String.class } );
        
            setMethod.invoke( ds, new Object[] { "upgrade=true" } );
        }
        
        Connection conn = ds.getConnection();
        
        return ds;
    }

    /**
     * <p>
     * Make a DataSource given a logical database name.
     * </p>
     */
    private DataSource makeDataSource( String logicalDatabaseName )
        throws Exception
    {
        return JDBCDataSource.getDataSourceLogical( logicalDatabaseName );
    }

    /**
     * <p>
     * Shutdown a database.
     * </p>
     */
    private void shutdownDatabase( DataSource ds )
    {
        JDBCDataSource.shutdownDatabase( ds );
    }
    
    /**
     * <p>
     * Verify that the database has the expected version.
     * </p>
     */
    private void vetDBVersion( Version softwareVersion, Version dataVersion, Connection conn )
        throws Exception
    {
        String expectedSoftwareVersion = softwareVersion.toString();
        String expectedDataVersion = dataVersion.getBranchID();
        String actualSoftwareVersion = trimDriverVersion( conn.getMetaData().getDriverVersion() );
        String actualDataVersion = getDataVersion( conn );

        assertEquals( _trajectoryName, expectedSoftwareVersion, actualSoftwareVersion );
        assertEquals( _trajectoryName, expectedDataVersion, actualDataVersion );
    }

    /**
     * <p>
     * Get the version of the data in the database.
     * </p>
     */
    private String getDataVersion( Connection conn )
        throws Exception
    {
        PreparedStatement ps = null;
        ResultSet               rs = null;

        try {
            ps = conn.prepareStatement( "values syscs_util.syscs_get_database_property('DataDictionaryVersion')" );
            rs = ps.executeQuery();

            rs.next();

            return rs.getString( 1 );
        }
        catch (SQLException se)
        {
            printStackTrace( se );
            return null;
        }
        finally
        {
            if ( rs != null ) { rs.close(); }
            if ( ps != null ) { ps.close(); }
        }
    }

    /**
     * <p>
     * Strip the trailing subversion stamp from a Derby version number.
     * </p>
     */
    private String trimDriverVersion( String driverVersion)
    {
        int idx = driverVersion.indexOf( ' ' );
        if ( idx > 0 ) { return driverVersion.substring( 0, idx ); }
        else { return driverVersion; }
    }

    private void saveOriginalClassLoader()
    {
        // remember the original class loader so that we can reset
//IC see: https://issues.apache.org/jira/browse/DERBY-6619
//IC see: https://issues.apache.org/jira/browse/DERBY-3745
        if ( _originalClassLoader.get() == null ) { 
            _originalClassLoader.set( ClassLoaderTestSetup.getThreadLoader() ); 
        }
    }
    private void restoreOriginalClassLoader()
    {
        ClassLoaderTestSetup.setThreadLoader(
                (ClassLoader) _originalClassLoader.get() );
    }

    private String stringifyUpgradeRequests()
    {
        StringBuffer buffer = new StringBuffer();
        int          count = _hardUpgradeRequests.length;

        buffer.append( "( " );

        for ( int i = 0; i < count; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            if ( _hardUpgradeRequests[ i ] ) { buffer.append( "hard" ); }
            else { buffer.append( "soft" ); }
        }

        buffer.append( " )" );

        return buffer.toString();
    }

    /**
     * Run good DDL.
     */
    protected void    goodStatement( Connection conn, String ddl ) throws SQLException
    {
        PreparedStatement    ps = chattyPrepare( conn, ddl );

        ps.execute();
        ps.close();
    }
    
    /**
     * Prepare a statement and report its sql text.
     */
    protected PreparedStatement   chattyPrepare( Connection conn, String text )
        throws SQLException
    {
        if ( LOQUACIOUS) { println( "Preparing statement:\n\t" + text ); }
        
        return conn.prepareStatement( text );
    }

    /**
     * Stringify a result set.
     */
	private	String	printResultSet( ResultSet rs )
		throws SQLException
	{
		if ( rs == null )
		{
			return "Null ResultSet!";
		}

		ResultSetMetaData	rsmd = rs.getMetaData();
		int					count = rsmd.getColumnCount();
		StringBuffer		buffer = new StringBuffer();

		while ( rs.next() )
		{
			for ( int i = 1; i <= count; i++ )
			{
                if ( i > 1 ) { buffer.append( " | " ); }
				buffer.append( rs.getString( i ) );
			}
			buffer.append( "\n" );
		}

		return buffer.toString();
	}

    /**
     * <p>
     * Replace values which are known to be unstable.
     * </p>
     */
    private void normalizeRow( String tableName, String[] columnNames, String[] row )
    {
        int count = row.length;

        for ( int i = 0; i < count; i++ )
        {
            String value = row[ i ];

            if ( isColumn( SYSCONGLOMERATES, CONGLOMERATENUMBER, tableName, columnNames[ i ] ) ) { value = DUMMY_NUMBER; }
            else if ( isColumn( SYSALIASES, ALIASID, tableName, columnNames[ i ] ) )
            {
                String original = value;
                value = getColumnValue( ALIAS, columnNames, row );
                _unstableColumnValues.put( original, value );
            }
            else if ( isColumn( SYSALIASES, SPECIFICNAME, tableName, columnNames[ i ] ) )
            {
                String original = value;
                value = getColumnValue( ALIAS, columnNames, row );
                _unstableColumnValues.put( original, value );
            }
            else if ( isColumn( SYSSTATEMENTS, STMTID, tableName, columnNames[ i ] ) )
            {
                String original = value;
                value = getColumnValue( STMTNAME, columnNames, row );
                _unstableColumnValues.put( original, value );
            }
            else if ( isColumn( SYSSTATEMENTS, LASTCOMPILED, tableName, columnNames[ i ] ) )
            {
                value = DUMMY_TIMESTAMP;
            }
            else if ( isColumn( SYSSTATEMENTS, TEXT, tableName, columnNames[ i ] ) && suffersDERBY_4216( tableName ) )
            {
                value = DUMMY_STRING;
            }
            else if ( isColumn( SYSROUTINEPERMS, ALIASID, tableName, columnNames[ i ] ) )
            {
                // replace with corresponding value that was substituted into SYSALIASES
                value = (String) _unstableColumnValues.get( value );
            }
            else if ( isColumn( SYSROUTINEPERMS, ROUTINEPERMSID, tableName, columnNames[ i ] ) )
            {
                value = DUMMY_STRING;
            }
            
            row[ i ] = value;
        }
    }

    /**
     * <p>
     * Return true if we are dealing with the indicated column.
     * </p>
     */
    private boolean isColumn( String expectedTableName, String expectedColumnName, String actualTableName, String actualColumnName )
    {
        return ( expectedTableName.equals( actualTableName ) && expectedColumnName.equals( actualColumnName ) );
    }
    
    /**
     * <p>
     * Return the value for the indicated columns.
     * </p>
     */
    private String getColumnValue( String columnName, String[] columnNames, String[] row )
    {
        int  count = columnNames.length;
        for ( int i = 0; i < count; i++ )
        {
            if ( columnName.equals( columnNames[ i ] ) ) { return row[ i ]; }
        }

        return null;
    }
    
    /**
     * <p>
     * Return true if the string looks like a UUID.
     * </p>
     */
    private boolean isUUID( String raw )
    {
        if (
            ( raw != null ) &&
            ( raw.length() == 36 ) &&
            ( raw.charAt( 8 ) == '-' ) &&
            ( raw.charAt( 13 ) == '-' ) &&
            ( raw.charAt( 18 ) == '-' )
            )
        { return true; }
        else { return false; }
    }

    /**
     * <p>
     * Debug method to print out a result set.
     * </p>
     */
    private void printQueryResults( Connection conn, String query )
        throws Exception
    {
        PreparedStatement ps = chattyPrepare( conn, query );
        ResultSet               rs = ps.executeQuery();

        if ( LOQUACIOUS) { println( printResultSet( rs ) ); }

        rs.close();
        ps.close();
    }

    /**
     * <p>
     * Debug method to compare the results of a query on two databases.
     * </p>
     */
    private void compareQueries( Connection leftConn, Connection rightConn, int colCount, String query )
        throws Exception
    {
        PreparedStatement leftPS = chattyPrepare( leftConn, query );
        PreparedStatement rightPS = chattyPrepare( rightConn, query );
        ResultSet               leftRS = leftPS.executeQuery();
        ResultSet               rightRS = rightPS.executeQuery();
        while( leftRS.next() )
        {
            rightRS.next();

            for ( int i = 1; i <= colCount; i++ )
            {
                String leftValue = leftRS.getString( i );
                String rightValue = rightRS.getString( i );
                boolean unequal = false;

                if ( leftValue == null )
                {
                    if ( rightValue != null ) { unequal = true; }
                }
                else if ( !leftValue.equals( rightValue ) ) { unequal = true; }

                if ( unequal )
                {
                    if ( LOQUACIOUS) { println( "Column values different for column " + i + ". Left = " + leftValue + ", right = " + rightValue ); }
                }
            }
        }

        leftRS.close();
        rightRS.close();
        leftPS.close();
        rightPS.close();
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // POWER SET BUILDING MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Builds the minimal set of trajectories for the supported versions. This
     * is all trajectories which start at some release, then hard upgrade to
     * every subsequent release up to the very last release. For a set of N
     * releases, there are N-1 non-vacuous trajectories of this shape.
     * </p>
     */
    private static Version.Trajectory[] buildMinimalSet( Version[] supportedVersions )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        ArrayList<Version.Trajectory>   trajectoryList = new ArrayList<Version.Trajectory>();
        int  versionCount = supportedVersions.length;
        boolean[]  include = new boolean[ versionCount ];

        for ( int i = 0; i < versionCount; i++ ) { include[ i ] = true; }
        addSubset( supportedVersions, trajectoryList, include, false ); 

        for ( int i = 0; i < versionCount - 1; i++ )
        {
            include[ i ] = false;
            addSubset( supportedVersions, trajectoryList, include, false ); 
        }

        return squeezeArray( trajectoryList );
    }
    
    /**
     * <p>
     * Build the power set of all supported versions.
     * </p>
     */
    private static Version.Trajectory[] buildPowerSet( Version[] supportedVersions )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        ArrayList<Version.Trajectory>   trajectoryList = new ArrayList<Version.Trajectory>();
        int            versionCount = supportedVersions.length;
        boolean[]  include = new boolean[ versionCount ];

        buildPowerSetMinion( supportedVersions, trajectoryList, include, 0, TRJ_SAME_BRANCH_NEIGHBORS );

        return squeezeArray( trajectoryList );
    }
    
    /**
     * <p>
     * Turn a list of trajectories into an array.
     * </p>
     */
    private static Version.Trajectory[] squeezeArray( ArrayList<Version.Trajectory> trajectoryList )
    {
        Version.Trajectory[] result = new Version.Trajectory[ trajectoryList.size() ];
        trajectoryList.toArray( result );

        return result;
    }
    
    /**
     * <p>
     * Recursive workhorse to build the power set of supported versions. If
     * requested, we also prune out all trajectories which have adjacent
     * versions from the same branch. If we're hard-upgrading, between releases,
     * then these SHOULD be uninteresting combinations.
     * </p>
     */
    private static void buildPowerSetMinion
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        ( Version[] supportedVersions, ArrayList<Version.Trajectory> result, boolean[] include, int idx, boolean removeSameBranchNeighbors )
    {
        int  versionCount = supportedVersions.length;

        if ( idx >= versionCount ) { addSubset( supportedVersions, result, include, removeSameBranchNeighbors ); }
        else
        {
            include[ idx ] = true;
            buildPowerSetMinion( supportedVersions, result, include, idx + 1, removeSameBranchNeighbors );
            
            include[ idx ] = false;
            buildPowerSetMinion( supportedVersions, result, include, idx + 1, removeSameBranchNeighbors );
        }
    }
    
    /**
     * <p>
     * Add a subset to the evolving list of subsets of supported versions. Throw
     * out the empty set and singletons. We sort each trajectory so that its
     * versions are in ascending order.
     * </p>
     */
    private static void addSubset
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        ( Version[] supportedVersions, ArrayList<Version.Trajectory> result, boolean[] include, boolean removeSameBranchNeighbors )
    {
        int  versionCount = supportedVersions.length;

        ArrayList<Version> seed = new ArrayList<Version>();
        Version   previousVersion = null;
        
        for ( int i = 0; i < versionCount; i++ )
        {
            Version  thisVersion = supportedVersions[ i  ];

            if ( include[ i ] )
            {
                //
                // If adjacent version are from the same branch, remove them if
                // requested to.
                //
                if ( removeSameBranchNeighbors && ( previousVersion != null ) )
                {
                    if ( previousVersion.getBranchID().equals( thisVersion.getBranchID() ) )
                    {
                        continue;
                    }
                }
                
                previousVersion = thisVersion;                
                seed.add( thisVersion );
            }
        }
        int  seedSize = seed.size();
        
        if ( seedSize > 1 )
        {
            Version[] subset = new Version[ seedSize ];
            seed.toArray( subset );
            
            result.add( (new Version.Trajectory( subset )).sort() );
        }
    }

    /**
     * <p>
     * Return empty string if passed in result is a known problem.
     * </p>
     */
    private String filterKnownProblems( String tableName, String actual )
    {
        if (
//IC see: https://issues.apache.org/jira/browse/DERBY-4157
//IC see: https://issues.apache.org/jira/browse/DERBY-4214
            _trajectory.endsAt( BRANCH_10_5 ) &&
            ( contains( actual, DERBY_4214_1 ) || contains( actual, DERBY_4214_2 ) )
           )
        { return ""; }

        if (
            suffersDERBY_4215( tableName ) &&
            ( contains( actual, DERBY_4215 ) )
           )
        { return ""; }

        return actual;
    }

    /**
     * <p>
     * Return true if the conditions of DERBY-4215 exist.
     * </p>
     */
    private boolean suffersDERBY_4215( String tableName )
    {
        return
                 (
                     SYSROUTINEPERMS.equals( tableName ) &&
                     _trajectory.startsAt( BRANCH_10_0 ) &&
                     ( !_trajectory.contains( BRANCH_10_1 ) ) &&
                     (
                      _trajectory.contains( BRANCH_10_2 ) ||
                      _trajectory.contains( BRANCH_10_3 ) ||
                      _trajectory.contains( BRANCH_10_4 )
                      ) &&
                     ( _trajectory.getEndingVersion().compareTo( VERSION_10_6_0_0 )  < 0 )
                 );
    }
    
    /**
     * <p>
     * Return true if the conditions of DERBY-4216 exist.
     * </p>
     */
    private boolean suffersDERBY_4216( String tableName )
    {
        return
                 (
                     SYSSTATEMENTS.equals( tableName ) &&
                     _trajectory.startsAt( BRANCH_10_0 ) &&
                     _trajectory.endsAt( BRANCH_10_1 )
                 );
    }
    
    private boolean contains( String left, String right )
    {
        return ( left.indexOf( right ) >= 0 );
    }
    
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // INNER CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////


}


