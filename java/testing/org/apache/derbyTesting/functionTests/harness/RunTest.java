/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.RunTest

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

package org.apache.derbyTesting.functionTests.harness;

import org.apache.derby.tools.sysinfo;
import org.apache.derby.tools.ij;
import org.apache.derby.iapi.reference.Attribute;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ByteArrayInputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.PrintStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.lang.ClassNotFoundException;
import java.lang.ClassFormatError;
import java.lang.Thread;
import java.lang.reflect.Method;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Vector;
import java.sql.Timestamp;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.StringTokenizer;
import java.net.URL;

import junit.framework.TestSuite;

public class RunTest
{

    // For printing debug info
    static boolean verbose=false;
    // Under some circumstances, we may need to skip the test
    static boolean skiptest = false;
    static StringBuffer skiptestReason = new StringBuffer();
    
    //java requires / to look into jars, irrespective of OS
    static final String testResourceHome = "/org/apache/derbyTesting/functionTests/";
    
    // Framework support
    static String[] validFrameworks = {"embedded","",
				       "DerbyNet","DerbyNetClient", "DB2jcc",
				       "DB2app"};
    static NetServer ns;
    static boolean serverNeedsStopping = false; // used in controlling network server when useprocess=false:
    static boolean jvmnet = false; // switch to see if we need have client & server in a different jvm
    static String jvmnetjvm; // string for class name of server jvm if different from client jvm
    static String driverName;
    static String dbName;

    // Test properties
	static String jvmName = "currentjvm";
	static String javaCmd;
	static String javaVersion; // System.getProperty("java.version")
	static String majorVersion;
	static String minorVersion;
	static int jccMajor;
	static int jccMinor;
	static int imajor;
	static int iminor;
	static boolean isjdk12test = false;
	static String classpath = "";
	static String classpathServer = "";
    public static String framework = "embedded";
    public static String J9_STATEMENTCACHESIZE = "20";

    static String usesystem = "";
    static String searchCP = "";
    static boolean useCommonDB = false;
	static boolean keepfiles = false;
	static boolean useprocess = true;
	static boolean systemdiff = false; // can set true if there is a system diff
	static boolean upgradetest = false;
	static boolean encryption = false; // requires jdk12ext plus encryptionProtocol
	static boolean jdk12exttest = false; // requires jdk12ext
    static boolean generateUTF8Out = false; // setting to create a utf8 encoded master file.
	static String runningdir = ""; // where the tests are run and suppfiles placed
	static String outputdir = ""; // user can specify as a property (optional)
	static String canondir; // optional (to specify other than "master")
	static String bootcp; // for j9 bootclasspath
	static String canonpath; // special full path (will be platform dependent)
	static String mtestdir = ""; // for MultiTest user must specify testdir
	static String testSpecialProps = ""; // any special suite properties
	static String testJavaFlags = ""; // special command line flags
	static String jvmflags; // java special flags
	static boolean reportstderr = true;
	static int timeout = -1; // in case tests are hanging
	public static String timeoutStr;
	static String jarfile; // some tests have jar files (like upgrade)
	static boolean skipsed = false;
	static String commonDBHome = "testCSHome";
	static boolean dbIsNew = true;
	static String runwithjvm="true";
	static boolean startServer=true; // should test harness start the server
	static String hostName; // needs to be settable for ipv testing, localhost otherwise.)
	static String testEncoding; // Encoding used for child jvm and to read the test output
	static String upgradejarpath; // Encoding used for child jvm and to read the test output
	static boolean replacePolicyFile=false; // property used to see if we need to replace the default policy file or append to it.

	// Other test variables for directories, files, output
	static String scriptName = ""; // testname as passed in
	static String scriptFileName; // testname with extension
	static String testDirName = ""; // test directory name
        static String defaultPackageName = "/org/apache/derbyTesting/";
        static String javaPath = "org.apache.derbyTesting."; // for java tests
	static String testType; // sql, java, unit, etc.
	static String testBase; // testname without extension
	static String testOutName; // output name without path or extension (optional)
	static String passFileName; // file listing passed tests
	static String failFileName; // file listing failed tests
    static String UTF8OutName; // file name for utf8 encoded out - not used for file comparison
	static String tempMasterName; //file name for master, converted to local encoding, and for network server, corrected master
	static File passFile;
    static File failFile;
	static String shutdownurl = "";
    static boolean useOutput; // use output or assume .tmp file is produced?
    static boolean outcopy; // copy support files to outDir rather than runDir
	static String userdir; // current user directory
	static char fileSep; // file separator for the system
    static PrintWriter printWriter = null; // used to write test output to .tmp
    static PrintWriter pwDiff = null; // for writing test output and info
    static File script; // The file created for test files other than java tests
    static File baseDir; // the DB base system dir
    static boolean deleteBaseDir; // the DB base system dir
    static File outDir; // test out dir
    static File runDir; // where test is run and where support files are expected
    static File canonDir; // allows setting master dir other than default
    static File tmpOutFile; // tmp output file (before sed)
    static File tempMasterFile; // master file copied into local encoding - with networkserver, also processed
    static File stdOutFile; // for tests with useoutput false
    static File finalOutFile; // final output file (after sed)
    static File UTF8OutFile; // file name for out file copied into utf8 encoding
    static File appPropFile; // testname_app.properties or default
    static File clPropFile; // testname_derby.properties or default
    static File diffFile; // To indicate diffs
    static File tsuiteDir; // Final output dir for suite(s)
    static File rsuiteDir; // Where to report .pass and .fail for suite(s)
	static File extInDir;    //Where all external test input files exist.
	static File extOutDir;    //Where all external test input files exist.
	static File extInOutDir;    //Where all external test input files exist.

    // This test may be part of a suite
    // (RunTest may need to know this is a suite to avoid dup output like sysinfo)
    static String topsuitedir = ""; // in case of nested suites
    static String topsuiteName = "";
    static String topreportdir = "";
    static String suiteName = "";
    static boolean isSuiteRun = false;
    static boolean lastTestFailed = false;

    static boolean isI18N = false;
    static boolean junitXASingle = false;
    
    /**
     * Run the test without a security manager. Hopefully
     * should only be used in few cases. Though initially
     * may be used to bypass problematic tests and get the
     * remainder of the tests running with the security manager.
     */
    static boolean runWithoutSecurityManager;

    static InputStream isSed = null; // For test_sed.properties // Cliff

	public static void main(String[] args)
		throws Exception
	{
		Locale.setDefault(Locale.US);
		skiptestReason.setLength(0); // 0 out for useprocess
		// Determine the test type
		if (args.length == 0)
		{
			// No script name provided
			System.out.println("no test name provided");
			System.exit(1);
		}
		scriptName = args[0];

		if (Boolean.getBoolean("listOnly")) 
		{
			System.out.println("LISTONLY :" + scriptName);
			return;
		}

		if ( (scriptName == null) || (scriptName.equals("")) )
		{
		    System.out.println("Null or blank test script name.");
		    System.exit(1);
		}
		// If useprocess=false RunList calls this main method with 7 arguments...
		if (args.length == 7)
		{
		    defaultPackageName = args[1];
		    usesystem = args[2];
		    useprocess = false;
		    shutdownurl = args[4];
		    isSuiteRun = true;
		    suiteName = args[5];
		    //System.out.println("suiteName: " + suiteName);
		    framework=args[6];		    
		    // initializing startServer to true (which it would be if we'd
		    // run with useprocess=true) or network server will not get started
		    startServer=true;		    
		}
		
		testType = scriptName.substring(scriptName.lastIndexOf(".") + 1);

		verifyTestType();

        // Get the properties for the test
		Properties sp = System.getProperties();

		// For useprocess=false, some system wide properties need to be reset
		if (useprocess == false)
		{
		    sp.put("useprocess", "false");
            // Reset maximumDisplayWidth because some tests set this in app properties
            // and when running in same process, can cause extra long lines and diffs
            if ( sp.getProperty("maximumDisplayWidth") == null )
                sp.put("maximumDisplayWidth", "128");
            if ( sp.getProperty("ij.defaultResourcePackage") != null )
                sp.put("ij.defaultResourcePackage", defaultPackageName);
            System.setProperties(sp);
        }
        
        JavaVersionHolder	jvhs = getProperties(sp);
		boolean				isJDBC4 = jvhs.atLeast( 1, 6 );

        // Setup the directories for the test and test output
        setDirectories(scriptName,sp);

        // Check for properties files, including derby.properties
        // and if needed, build the -p string to pass to the test
        String propString = createPropString();
        if ( (isSuiteRun == false) && (useprocess) )
        {
            SysInfoLog sysLog = new SysInfoLog();
            sysLog.exec(jvmName, javaCmd, classpath, framework, pwDiff, useprocess);
        }

	    String startTime = CurrentTime.getTime();
	    StringBuffer sb = new StringBuffer();
	    sb.append("*** Start: " + testBase + " jdk" + javaVersion + " ");
	    if ( (framework.length()>0) && (!framework.startsWith("embedded")) )
	        sb.append(framework + " ");
	    if ( (suiteName != null) && (suiteName.length()>0) )
	        sb.append(suiteName + " ");
	    sb.append(startTime + " ***");
	    System.out.println(sb.toString());
	    pwDiff.println(sb.toString());

            // Run the Server if needed
	    if ((driverName != null) && (!skiptest) )
	    {
            // before going further, get the policy file copied and if
            // needed, modify it with the test's policy file
            composePolicyFile();
            String spacedJvmFlags = jvmflags;
            // we now replace any '^' in jvmflags with ' '
            if ((jvmflags != null) && (jvmflags.indexOf("^")>0))
            {
                spacedJvmFlags = spaceJvmFlags(jvmflags);   
            }
            
            System.out.println("Initialize for framework: "+ framework );
            if (jvmnet && framework.startsWith("DerbyNet"))
            {
                // first check to see if properties were set to use a different jvm for server/client
                String jvmnetjvm = System.getProperty("serverJvmName");
                if (jvmnetjvm == null) 
                {
                    // default to the latest one we know 
                    jvmnetjvm = "j9_22";
                }

                ns = new NetServer(baseDir, jvmnetjvm, classpathServer, null,
                                     spacedJvmFlags,framework, startServer);
            }
            else
                ns = new NetServer(baseDir, jvmName, classpathServer, 
                                     javaCmd, spacedJvmFlags,framework, startServer);

            //  With useprocess=true, we have a new dir for each test, and all files for
            // the test, including a clean database, go in that directory. So, network server
            // for each test runs in that dir, and stops when done. If the test's properties
            // file has startServer=false the test will handle start/stop, otherwise the harness
            // needs to start and stop the server.
            //  But with useprocess=false we're using the same directory and thus the same
            // database, so there's little point in bouncing the server for each test in a suite,
            // and it would slow the suite run.
            //  So, with useprocess=true, or if we're just running 1 test with useprocess=false,
            // start network server and set serverNeedsStopping=true to have it stopped later; 
            // if useprocess=false and we're in a suite, start network server if it's not running 
            // and leave serverNeedsStopping=false, unless startServer=false, then, 
            // if network server is running, stop it.
            if ((!useprocess) && (isSuiteRun))
            {            	   
                boolean started = false;
                try 
                {
                    started = ns.testNetworkServerConnection();
                }
                catch (Exception e) {} // ignore
                if (!started && startServer)
                    ns.start(); // start but don't stop, so not setting serverNeedsStopping
                if (started && !startServer)
                    ns.stop();
            }
            else
            {
                ns.start(); 
                serverNeedsStopping = true;
            }
        }


		
        // If the test has a jar file (such as upgrade) unjar it
        if (jarfile != null)
        {
            UnJar uj = new UnJar();
            uj.unjar(jarfile, outDir.getCanonicalPath(), true);
        }
        
        // Run the actual test (unless skiptest was set to true)
        if (skiptest == false)
        {
            testRun(propString, sp);
        }
        else
        {
			if (skiptestReason.length() == 0)
				addSkiptestReason("Test skipped: skiptest set without setting skiptestReason, please fix RunTest.java...");
		    pwDiff.println(skiptestReason);
		    System.out.println(skiptestReason);
            doCleanup(javaVersion);
            return;
        }
            
        // Stop the Network server if necessary
		if (serverNeedsStopping)
		{
		    ns.stop();
		}

		// Do "sed" to strip some unwanted stuff from the output file
		// unless flag skipsed is set to true (for special cases)
		
		String outName = finalOutFile.getPath();

        if (testDirName.startsWith("i18n"))
        {
            isI18N=true;
        }
        
        if (skipsed)
        {
            tmpOutFile.renameTo(finalOutFile);
        }
        else
        {
                    try
                    {
                        Sed sed = new Sed();
                        sed.exec(tmpOutFile,finalOutFile, isSed, 
                                        NetServer.isClientConnection(framework), isI18N, isJDBC4);
		    }
		    catch (ClassFormatError cfe)
		    {
		        if (verbose) System.out.println("SED Error: " + cfe.getMessage());
		    }
        }
        // Now do a diff between the out and the master files
    	// Use the system's diff if systemdiff is true
    	String frameworkMaster = framework;
    	if (framework.startsWith("embedded"))
    	    frameworkMaster = "";
    	FileCompare diff = new FileCompare();

        if (verbose)
        {
            System.out.println(
                "About to execute: diff.exec(" + 
                " outName = "           + outName +
                ",outDir = "            + outDir  +
                ",pwDiff = "            + pwDiff  +
                ",testOutName = "       + testOutName +
                ",frameworkMaster = "   + frameworkMaster +
                ",jvmName = "           + jvmName +
                ",iminor = "            + iminor  +
                ",useprocess = "        + useprocess +
                ",systemdiff = "        + systemdiff +
                ",canondir = "          + canondir +
                ",canonpath = "         + canonpath +
                ")\n");
        }

    	boolean status;

    	// allow for server jvmName to be different from client jvmName
    	if (jvmnet)
    	{
            // first check to see if properties were set to use a different jvm for server/client
            if (jvmnetjvm == null) 
            {
                // default to the latest one we know 
                jvmnetjvm = "j9_22";
            }
    	    status = diff.exec(outName, outDir, pwDiff, testOutName,
    		    frameworkMaster, jvmName, iminor, useprocess, systemdiff, canondir, 
			    canonpath, jvmnetjvm);
        }
    	else
      	    status = diff.exec(outName, outDir, pwDiff, testOutName,
    		    frameworkMaster, jvmName, iminor, useprocess, systemdiff, canondir, 
			    canonpath, null);

    	if (status == true)
    	{
    		lastTestFailed = true;
    		pwDiff.println("Test Failed.");
    		System.out.println("Test Failed.");
    		keepfiles = true;
		    addToFailures(scriptName);
		    if (useCommonDB) {
	    		status = baseDir.delete();
//System.out.println("basedir delete status: " + status );
    		}
        }
        else
    	{
			addToSuccesses(scriptName);
    		pwDiff.flush();
        }

        generateUTF8OutFile(finalOutFile);
        
		// Cleanup files
		doCleanup(javaVersion);
	}

    private static void testRun(String propString, Properties sysProp)
        throws FileNotFoundException, IOException, Exception
    {
        String systemHome = baseDir.getPath();
        String scriptPath = null;
        if (testType.startsWith("sql"))
            scriptPath = script.getPath();
                
	    // cleanup for all tests that re-use standard testCSHome/wombat database
	    if (useCommonDB == true 
	    	&& (usesystem == null || usesystem == "")
	    	&& (testType.equals("sql") || testType.equals("java")
			|| testType.equals("sql2"))) {
	        dbcleanup.doit(dbIsNew);
	    }
	

        // Create a process to execute the command unless useprocess is false
        if ( useprocess )
		{
            // Build the test command
            String[] testCmd = 
        		buildTestCommand(propString, systemHome, scriptPath);
            execTestProcess(testCmd);
		}
        else
		{
            execTestNoProcess(sysProp, systemHome, propString, scriptPath);
		}
    }

    /** This is the method which created directories and looks for script file,
     * need to make OS specific paths here.
     *
     */
    private static void setDirectories(String scriptName, Properties sp)
        throws ClassNotFoundException, FileNotFoundException, IOException
    {
        // Get the current userdir
        userdir = sp.getProperty("user.dir");
        
        // reset defaultPackageName (for useprocess=false)
        if (useprocess == false)
          defaultPackageName = "/org/apache/derbyTesting/";
            
        // reset defaultPackageName (for useCommonDB=true)
        if (useCommonDB == true)
        {
            defaultPackageName = "/org/apache/derbyTesting/";
        }
            
        // Set the resourceName from the default
        // If not set by user, the default is used
        String resourceName = defaultPackageName + "functionTests/tests/";
        // scriptName could be of these two formats:
        // testdir/test.testtype (testtype is sql, java, etc.)
        // test.testtype (where the defaultPackageName includes the testdir)
        int index = scriptName.lastIndexOf('/');
        if (index == -1) // no test directory was specified
        {
            if ( (!testType.equals("sql")) && (!testType.equals("java")) && (!testType.equals("junit")))
            {
                System.out.println("Test argument should be of the form: <dir>/<test>.<ext>");
                System.exit(1);
            }
            else
            {
                scriptFileName = scriptName; // such as my.sql
                resourceName += scriptName; // build the full resource name
            }
        }
        else // the testdir was specified 
        {
            testDirName = (index==0)?"":scriptName.substring(0,index);
            //System.out.println("testDirName: " + testDirName);
            scriptFileName = scriptName.substring(index+1, scriptName.length());
            //System.out.println("scriptFileName: " + scriptFileName);
            if (testType.equals("multi"))
                defaultPackageName = defaultPackageName + "functionTests/multi/" + testDirName + "/";
            else
                defaultPackageName = defaultPackageName + "functionTests/tests/" + testDirName + "/";
            //System.out.println("defaultPackage: " + defaultPackageName);
            resourceName = defaultPackageName + scriptFileName;
            //System.out.println("resource: " + resourceName);
        }


        // Get the test name without the extension
        testBase = scriptFileName.substring(0, scriptFileName.lastIndexOf("."+testType));

		if (testType.equals("java") || testType.equals("junit"))
		{
                    //get the javaPath
		    String tmp = defaultPackageName.replace('/', '.');
                    int tl = tmp.length()-1;
		    javaPath = (tl==0)?"":tmp.substring(1, tl);
		}

        // Check for runDir
        if ( (runningdir != null) && (runningdir.length()>0) )
        {
            if (File.separatorChar == '\\')
            {
                //need to replace / in path with \ for windows
                String runningdirWin = convertPathForWin(runningdir);
                runDir = new File((new File(runningdirWin)).getCanonicalPath());
            }
            else
            {
                runDir = new File((new File(runningdir)).getCanonicalPath());
            }
        }

        // Define the outDir if not already defined from properties
        File tmpoutDir;
        String userdirWin = null;
            
        if ( (outputdir == null) || (outputdir.length()==0) )
        {
        	if (File.separatorChar == '\\')
            {
                //need to replace / in path with \ for windows
                userdirWin = convertPathForWin(userdir);
                tmpoutDir = new File((new File(userdirWin)).getCanonicalPath());
            }
            else
            {
                tmpoutDir =
		        new File((new File(userdir)).getCanonicalPath());
            }
        }       
        else
        {
            if (File.separatorChar == '\\')
            {
                String outputdirWin = convertPathForWin(outputdir);
                tmpoutDir =
                        new File((new File(outputdirWin)).getCanonicalPath());
            }
            else
            {
                tmpoutDir =
                        new File((new File(outputdir)).getCanonicalPath());
            }
        }

        // If this is a suite run in a framework, outdir
        // would already be defined to be a framework subdir
        // But for RunTest, we must create the framework subdir
        //if ( (!isSuiteRun) && (framework != null) && (framework.length()>0) )
        if ( (!isSuiteRun) && (!framework.startsWith("embedded")) )
        {
            runDir = tmpoutDir;
		    outDir = new File(tmpoutDir, framework);
		    outDir.mkdir();
		}
		else // This is a Suite Run
		{
		    outDir = tmpoutDir;
		    outDir.mkdir();
		    if ( (topsuitedir != null) && (topsuitedir.length()>0) )
                    {
                        if (File.separatorChar == '\\')
                        {
                            String topsuitedirWin = convertPathForWin(topsuitedir);
                            tsuiteDir = 
                                new File((new File(topsuitedirWin)).getCanonicalPath());
                        }
                        else
                        {
                            tsuiteDir = new File((new File(topsuitedir)).getCanonicalPath());
                        }
                    }
		    else
                    {
		        tsuiteDir = outDir;
                    }
		    tsuiteDir.mkdir();
		    if ( (topreportdir != null) && (topreportdir.length()>0) )
                    {
                        if (File.separatorChar == '\\')
                        {
                            String topreportdirWin = convertPathForWin(topreportdir);
                            rsuiteDir = 
                                new File((new File(topreportdirWin)).getCanonicalPath());
                        }
                        else
                        {
                            rsuiteDir = 
                                new File((new File(topreportdir)).getCanonicalPath());
                        }
                    }
		    else
                    {
		        rsuiteDir = outDir;
                    }
		    rsuiteDir.mkdir();		    
		}
		        
	    fileSep = File.separatorChar;

        // For multi tests, the user should have specified mtestdir (full path)
        // unless this is a Suite, in which case outDir is used for mtestdir
        if ( testType.equals("multi") )
            if ( (mtestdir == null) || (mtestdir.length()==0) )
                // Use outDir for mtestdir
                mtestdir = outDir.getPath();

        // For certain test types, locate script file based on scriptName
        // Then determine the actual test name and directory
        if ( (!testType.equals("java")) &&
             (!testType.equals("junit")) &&
             (!testType.equals("unit")) && 
             (!testType.equals("multi")) ) 
        {
            // NOTE: cannot use getResource because the urls returned
            // are not the same between different java environments
            InputStream is =
                loadTestResource("tests/" + testDirName + "/" + scriptFileName); 

			if (is == null)
			{
			    System.out.println("Could not locate " + scriptName);
			    addToFailures(scriptName);
				throw new FileNotFoundException(resourceName);
			}

            // Read the test file and copy it to the outDir
            // except for multi tests (for multi we just need to locate it)
            BufferedReader in = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            if (upgradetest)
		
                //these calls to getCanonicalPath catch IOExceptions as a workaround to
                //a bug in the EPOC jvm. 
                try { script = new File((new File(userdir, scriptFileName)).getCanonicalPath()); }
                catch (IOException e) {
                    File f = new File(userdir, scriptFileName);
        		    FileWriter fw = new FileWriter(f);
                    fw.close();
                    script = new File(f.getCanonicalPath());
                }
			// else is probably only multi test
            else
                try { script = new File((new File(outDir, scriptFileName)).getCanonicalPath()); } 
                catch (IOException e) {
                    File f = new File(outDir, scriptFileName);
                    FileWriter fw = new FileWriter(f);
                    fw.close();
                    script = new File(f.getCanonicalPath());
                }

            PrintWriter pw = null;
            pw = new PrintWriter( new BufferedWriter
                (new FileWriter(script.getPath()), 10000), true );
                
            String str = "";
            while ( (str = in.readLine()) != null )
            {
                pw.println(str);
            }
            pw.close();
            pw = null;
            in = null;
        }

        // This is the base directory for creating a database (under the outDir)
        baseDir = null;

    	if (useCommonDB == true)
        {
            if (File.separatorChar == '\\')
            {
                String commonDBHomeWin = convertPathForWin(commonDBHome);
                baseDir = new File(userdirWin, commonDBHomeWin);
            }
            else
            {
    		baseDir = new File(userdir, commonDBHome);
            }
        }
        else if ( (!useprocess) && isSuiteRun && ((usesystem==null) || (usesystem.length()<=0)) )
        {
            String suite = (suiteName.substring(0,suiteName.indexOf(':')));
            if (File.separatorChar == '\\')
            {
                String useprWin = convertPathForWin(suite);
                baseDir = new File(outDir, useprWin);
            }
            else
            {
                baseDir = new File(outDir, suite);
            }
        }
        else if ( (usesystem != null) && (usesystem.length()>0) )
        {
            if (File.separatorChar == '\\')
            {
                String usesystemWin = convertPathForWin(usesystem);
                if (upgradetest == true)
                    baseDir = new File(userdirWin, usesystemWin);
                else
                    baseDir = new File(outDir, usesystemWin);
            }
            else
            {
                if (upgradetest == true)
                    baseDir = new File(userdir, usesystem);
                else
                    baseDir = new File(outDir, usesystem);
            }
        }
        else
        {
            if (File.separatorChar == '\\')
            {
                String testBaseWin = convertPathForWin(testBase);
                baseDir = new File(outDir, testBaseWin);
            }
            else
            {
                baseDir = new File(outDir, testBase);
            }
        }

        // clean up old db dirs
        // (except for special cases such as nist, commonDB).
        // In the case of useCommonDB == true, the baseDir (commonDBHome) only gets
        // cleaned up if the last test was a failure.  Further refinements may
        // follow, since many test failures probably do not require such drastic
        // action.
        if (baseDir.exists())
        {
            if (useCommonDB == false || lastTestFailed == true) {
                cleanupBaseDir(baseDir);
                lastTestFailed = false;
            }
            else if (useCommonDB == true) 
                dbIsNew = false; // dbcleanup may be needed
        }
        else {
            
            boolean created = baseDir.mkdir();
            dbIsNew = true; // dbcleanup not needed on new database
        }

        // Determine if it is ok to delete base when done
        if ( (usesystem == null) || (usesystem.length()<=0) )
            deleteBaseDir = true; // ok to delete base when done
        else
            deleteBaseDir = false; // keep db dir for nist & puzzles in case of failures


        // testOutName used to create the tmpOutFile
        // this is probably always going to be testBase
        if ( testOutName == null )
        {
            if (testType.equals("demo"))
                testOutName = testBase.substring(testBase.indexOf(".")+1);
            else
                testOutName = testBase;
        }

        // Create a .tmp file for doing sed later to create testBase.out
        tmpOutFile = new File(outDir, testOutName + ".tmp");
        // Always create a.tmpmstr copy of the master file in local encoding.
        // With network server, this gets adjusted for displaywidth 
        tempMasterName = testOutName+".tmpmstr";
		UTF8OutName = testOutName+".utf8out";
		// Define the .out file which will be created by massaging the tmp.out
		finalOutFile = new File(outDir, testOutName + ".out");

		// Define the .diff file which will contain diffs and other info
		diffFile = new File(outDir, testOutName + ".diff");
		stdOutFile = new File(outDir, testOutName + ".std");

        // Define also the .pass and .fail files        
		if ( isSuiteRun ) 
		{
		    String sname = suiteName.substring(0,suiteName.indexOf(":"));
		    //System.out.println("sname: " + sname);
		    //System.out.println("topsuiteName: " + topsuiteName);
		    passFileName = sname+".pass";
		    passFile = new File(rsuiteDir, passFileName);
		    failFileName = sname+".fail";
		    failFile = new File(rsuiteDir, failFileName);
		}
		else
		{
		    passFileName=testBase+".pass";
	        passFile = new File(outDir, passFileName);
	        failFileName=testBase+".fail";
	        failFile = new File(outDir, failFileName);
	    }
        //System.out.println("passFileName: " + passFileName);
        boolean status = true;

        // Delete any old .out or .tmp files
        if (tmpOutFile.exists())
            status = tmpOutFile.delete();
        tempMasterFile = new File(outDir, tempMasterName);
        if (tempMasterFile.exists())
            status = tempMasterFile.delete();
		if (finalOutFile.exists())
            status = finalOutFile.delete();
        if (diffFile.exists())
            status = diffFile.delete();
        if (stdOutFile.exists())
            status = stdOutFile.delete();
        UTF8OutFile = new File (outDir, UTF8OutName);
        if (UTF8OutFile.exists())
            status = UTF8OutFile.delete();

        // Delete any old pass or fail files
        if (!isSuiteRun)
        {
            if (failFile.exists())
                status = failFile.delete();
            if (passFile.exists())
                status = passFile.delete();
        }

        if (status == false)
            System.out.println("Unable to delete tmp, out and/or diff files to start");

        // Create a PrintWriter for writing env and test info to the diff file
        pwDiff = new PrintWriter
            (new BufferedWriter(new FileWriter(diffFile.getPath()), 4096), true);
    }

    private static JavaVersionHolder getProperties(Properties sp)
        throws Exception
    {
        // Get any properties specified on the command line
        
        // before doing anything else, get jvmflags, evaluate any -D 
        // see if there is anything useful to the test harness in jvmflags
        if ((jvmflags != null) && (jvmflags.length() > 0))
        {
            StringTokenizer st = new StringTokenizer(jvmflags,"^");
            while (st.hasMoreTokens())
            {
                String tmpstr = st.nextToken();
                if ((tmpstr.indexOf("=")> 0) && (tmpstr.startsWith("-D")))
                {
                    // strip off the '-D'
                    String key = tmpstr.substring(2, tmpstr.indexOf("="));
                    String value = tmpstr.substring((tmpstr.indexOf("=") +1), tmpstr.length());
                    sp.put(key, value);
                }
            }
        }
        
        searchCP = sp.getProperty("ij.searchClassPath");
		String frameworkp = sp.getProperty("framework");
		if (frameworkp != null)
			framework = frameworkp;
		if (framework == null)
			framework = "embedded";
		if (!verifyFramework(framework))
			framework = "";
		else
			driverName = NetServer.getDriverName(framework);
        String junitXAProp = sp.getProperty ("derbyTesting.xa.single");
        if (junitXAProp != null && junitXAProp.equals ("true")) {
            junitXASingle = true;
        }
        hostName = sp.getProperty("hostName");
        // force hostName to localhost if it is not set
        if (hostName == null)
           hostName="localhost";
		
        String generateUTF8OutProp = sp.getProperty("generateUTF8Out");
        if (generateUTF8OutProp != null && generateUTF8OutProp.equals("true"))
        	generateUTF8Out = true;
        
        // Some tests will not work with some frameworks,
        // so check suite exclude files for tests to be skipped
        String skipFile = framework + ".exclude";
        if (!framework.equals(""))
        {
            skiptest = (SkipTest.skipIt(skipFile, scriptName));
            // in addition, check to see if the test should get skipped 
            // because it's not suitable for a remotely started server
            if (!skiptest) 
            {
                if (!hostName.equals("localhost")) 
                {
                    skipFile = framework + "Remote.exclude";
                    skiptest = (SkipTest.skipIt(skipFile, scriptName));
                }
            }
            if (skiptest) // if we're skipping...
                addSkiptestReason("Test skipped: listed in " + skipFile + 
                     " file, skipping test: " + scriptName);
        }
		
		jvmName = sp.getProperty("jvm");

		//System.out.println("jvmName is: " + jvmName);
		if ( (jvmName == null) || (jvmName.length()==0) || (jvmName.equals("jview")))
		{
		    javaVersion = System.getProperty("java.version");
		    //System.out.println("javaVersion is: " + javaVersion);
		}
		else
		    javaVersion = jvmName;

		//hang on a minute - if j9, we need to check further
		String javavmVersion;
		if (sp.getProperty("java.vm.name").equals("J9"))
			javavmVersion = (sp.getProperty("java.vm.version"));
		else
			javavmVersion = javaVersion;


		JavaVersionHolder jvh = new JavaVersionHolder(javavmVersion);
		majorVersion = jvh.getMajorVersion();
		minorVersion = jvh.getMinorVersion();
        iminor = jvh.getMinorNumber();
        imajor = jvh.getMajorNumber();

		if ( (jvmName == null) || (!jvmName.equals("jview")) )
		{
		    if ( (iminor < 2) && (imajor < 2) )
		        jvmName = "currentjvm";
		    else
		    {
                if (System.getProperty("java.vm.vendor").startsWith("IBM"))
                {
                    if (System.getProperty("java.vm.name").equals("J9"))
                    {
                        if (System.getProperty("com.ibm.oti.configuration").equals("foun10"))
                        {
                            jvmName = "j9_foundation";
                        }
                        else
                        {
                            // for reporting; first extend javaVersion
                            javaVersion = javaVersion + " - " + majorVersion + "." + minorVersion;
                            // up to j9 2.1 (jdk 1.3.1 subset) the results are the same for all versions,
                            // or we don't care about it anymore. Switch back to 1.3. (java.version) values.
                            if ((imajor <= 2) && (iminor < 2)) 
                            { 
                                majorVersion = "1"; 
                                minorVersion = "3"; 
                                imajor = 1; 
                                iminor = 3; 
                            } 
                            jvmName = "j9_" + majorVersion + minorVersion;
                        }
                    }
                    else
                        jvmName = "ibm" + majorVersion + minorVersion;
                }
                else
  		    jvmName = "jdk" + majorVersion + minorVersion;
            }
        }

        // create a JavaVersionHolder for the java.specification.version - 
        // used to control Sed-ing for JDBC4 & up
        String specversion = (sp.getProperty("java.specification.version"));
        JavaVersionHolder jvhs = new JavaVersionHolder(specversion);

        testEncoding = sp.getProperty("derbyTesting.encoding");
        upgradejarpath = sp.getProperty("derbyTesting.jar.path");
        if ((testEncoding != null) && (!jvmName.equals("jdk15")))
        {
            skiptest = true;
            addSkiptestReason("derbyTesting.encoding can only be used with jdk15, skipping test");
        }
		
        String replace_policy = sp.getProperty("derbyTesting.replacePolicyFile");
        if ((replace_policy != null) && (replace_policy.equals("true")))
            replacePolicyFile=true;
        else
            replacePolicyFile=false;
        
        javaCmd = sp.getProperty("javaCmd");
        bootcp = sp.getProperty("bootcp");
        jvmflags = sp.getProperty("jvmflags");
		testJavaFlags = sp.getProperty("testJavaFlags");
		classpath = sp.getProperty("classpath");
		//System.out.println("classpath set to: " + classpath);
		classpathServer = sp.getProperty("classpathServer");
		if ( (classpathServer == null) || (classpathServer.startsWith("${")) )
		    classpathServer = classpath;
		//System.out.println("classpathServer set to: " + classpathServer);
		jarfile = sp.getProperty("jarfile");
		String upg = sp.getProperty("upgradetest");
		if (upg != null)
		{
		    upg = upg.toLowerCase();
		    if (upg.equals("true"))
		        upgradetest = true;
		}
	
        if ( framework.equals("DerbyNet") && (! jvmName.equals("j9_foundation")))
		{	

			Class c = null;
			Method m = null;
			Object o = null;
			Integer i = null;
 			try	
			{
				c = Class.forName("com.ibm.db2.jcc.DB2Driver");
				o = c.newInstance();
				m = c.getMethod("getMajorVersion", null);
				i = (Integer)m.invoke(o, null);
				jccMajor = i.intValue();
				m = c.getMethod("getMinorVersion", null);
				i = (Integer)m.invoke(o, null);
				jccMinor = i.intValue();
			} catch (ClassNotFoundException e) {}

			String excludeJcc = sp.getProperty("excludeJCC");
			try {
				RunList.checkClientExclusion(excludeJcc, "JCC", jccMajor, jccMinor, javaVersion);
			} catch (Exception e) {
				skiptest = true;
				addSkiptestReason(e.getMessage());
			}
		}
			
		String sysdiff = sp.getProperty("systemdiff");
		if (sysdiff != null)
		{
		    sysdiff = sysdiff.toLowerCase();
		    if (sysdiff.equals("true"))
		        systemdiff = true;
		}
		String keep = sp.getProperty("keepfiles");
		if (keep != null)
		{
		    keep = keep.toLowerCase();
		    if (keep.equals("true"))
		        keepfiles = true;
		}
		String encrypt = sp.getProperty("encryption");
		if ( (encrypt != null) && (encrypt.equalsIgnoreCase("true")) )
		    encryption = true;
		String jdk12ext = sp.getProperty("jdk12exttest");
		if ( (jdk12ext != null) && (jdk12ext.equalsIgnoreCase("true")) )
		    jdk12exttest = true; // applied to jdk12 or higher
		if ( encryption || jdk12exttest )
		{
		    // Must be running jdk12 or higher and must have extensions
		    if ( iminor < 2 ) // this is 1.1.x
			{
		        skiptest = true;
				addSkiptestReason("Test skipped: encryption or jdk12exttest requires jdk12 or higher; this is jdk1"+iminor+", skipping test: " + scriptFileName);
			}
		    else // now check for extensions
		    {
			    try
			    {
                    Class jtaClass = Class.forName("javax.transaction.xa.Xid");
                } 
                catch (ClassNotFoundException cnfe)
                {
                    // at least one of the extension classes was not found
                    skiptest = true;
				    addSkiptestReason("Test skipped: javax.transaction.xa.Xid not found, skipping test: " + scriptFileName);
                }			
			    try
			    {
                    Class jdbcClass = Class.forName("javax.sql.RowSet");
                } 
                catch (ClassNotFoundException cnfe2)
                {
                    // at least one of the extension classes was not found
                    skiptest = true;
				    addSkiptestReason("Test skipped: javax.sql.RowSet not found, skipping test: " + scriptFileName);
                }			
		    }
		}
		runningdir = sp.getProperty("rundir");
		if (runningdir == null)
		    runningdir = "";
		outputdir = sp.getProperty("outputdir");
		if (outputdir == null)
		    outputdir = "";
		
		canondir = sp.getProperty("canondir");
		canonpath = sp.getProperty("canonpath");

		testOutName = sp.getProperty("testoutname");
		useOutput = new Boolean(sp.getProperty("useoutput","true")).booleanValue();
		outcopy = new Boolean(sp.getProperty("outcopy","false")).booleanValue();
		mtestdir = sp.getProperty("mtestdir"); // used by multi tests
		if (mtestdir == null)
		    mtestdir = "";
		    
		String usepr = sp.getProperty("useprocess");
		if (usepr != null)
		{
		    usepr = usepr.toLowerCase();
		    if (usepr.equals("false"))
		        useprocess = false;
		    else
		        useprocess = true;
		}
		else
		    useprocess = true;
		
        
		// if the hostName is something other than localhost, we must
		// be trying to connect to a remote server, and so, 
		// startServer should be false.
		if (!hostName.equals("localhost"))
		{
        	    startServer=false;
		}
		
		String nosed = sp.getProperty("skipsed");
		if (nosed != null)
		{
		    nosed = nosed.toLowerCase();
		    if (nosed.equals("true"))
		        skipsed = true;
		}

		    
		String dbug = sp.getProperty("verbose");
		if (dbug != null)
		{
		    dbug = dbug.toLowerCase();
		    if (dbug.equals("true"))
		        verbose = true;
		}
		String rstderr = sp.getProperty("reportstderr");
		if (rstderr != null)
		{
		    rstderr = rstderr.toLowerCase();
		    if (rstderr.equals("false"))
		        reportstderr = false;
		}

		// default to -1 (no timeout) if no property is set
		if (timeoutStr == null)
		{
			timeoutStr = sp.getProperty("timeout", "-1");
            //System.out.println("+++setting timeoutStr to " + timeoutStr + " in RunTest::getProperties");
		}
        else
        {
            //System.out.println("+++timeoutStr was already " + timeoutStr + " in RunTest::getProperties");
        }
        
		try
		{
			timeout = Integer.parseInt(timeoutStr);
		}
		catch (NumberFormatException nfe)
		{
			timeout = -1;
		}
        
        //System.out.println("RunTest timeout is: " + timeout);
        
		testSpecialProps = sp.getProperty("testSpecialProps");
		
		if (useprocess)
		{
		    String defrespckg = sp.getProperty("ij.defaultResourcePackage");
		    if (defrespckg != null) // if not set there is a default defined
		    {
		        defaultPackageName = defrespckg;
		        if (!defaultPackageName.endsWith("/"))
		            defaultPackageName += "/";
		    }
		    usesystem = sp.getProperty("usesystem");
		}

		// Some tests will not run well in a suite with use process false 
		// with some frameworks, so skip
		if (!useprocess && !skiptest )
		{
		    String tsuiteName = null;
		    if (suiteName != null) 
		        tsuiteName = suiteName; 
		    else
		        tsuiteName = sp.getProperty("suitename");
		    if ( (tsuiteName != null) && (tsuiteName.length()>0) )
		    {	                
		        skipFile = framework + "Useprocess.exclude";
		        if (!framework.equals(""))
		        {
		            skiptest = (SkipTest.skipIt(skipFile, scriptName));
		            if (skiptest) 
		            {
		                skiptest=true;
		                addSkiptestReason("Test " + scriptName + " skipped, " +
                   		   "listed in " + framework + "Useprocess.exclude file.");  				      
		            }
		        }
		    }	                
		}
		
		if ( (useprocess) || (suiteName == null) )
		{
		    if (useprocess)
		    	suiteName = sp.getProperty("suitename");
		    if ( (suiteName != null) && (suiteName.length()>0) )
		    {
		        // This is a suite run
		        isSuiteRun = true;		        
		        if (useprocess) 
		            // If a suite, it could be part of a top suite
		            topsuiteName = sp.getProperty("topsuitename");
		        topsuitedir = sp.getProperty("topsuitedir");
		        topreportdir = sp.getProperty("topreportdir");
		    }
		}
		String uscdb = sp.getProperty("useCommonDB");
		if (uscdb != null && uscdb.equals("true"))
			useCommonDB = true;

		return jvhs;
    }

    private static String createPropString()
        throws ClassNotFoundException, FileNotFoundException, IOException
    {
        // Check for existence of app properties and/or derby.properties files
        // Copy the derby.properties to the db base directory
        // Then create the -p string for the test
		String propString = "";
		
		// General purpose variables
		BufferedReader in = null;
		BufferedOutputStream bos = null;
		BufferedOutputStream bos2 = null;
		String str = "";
		
		// InputStreams for all possible properties files
		InputStream isCl = null; // For test_derby.properties
		InputStream isClDef = null; // For default_derby.properties
		InputStream isAp = null; // For test_app.properties
		InputStream isApDef = null; // For default_app.properties

		// Resource names for locating the various properties files
		String clDefProp = "tests/" + testDirName + "/" + "default_derby.properties";
		String apDefProp = null;
		if (useCommonDB)
		    apDefProp = "tests/" + testDirName + "/" + "commonDB_app.properties";
		else
		    apDefProp = "tests/" + testDirName + "/" + "default_app.properties" ;

		// Properties
		Properties clp = new Properties();
		Properties ap = new Properties();

        // If there are special flags for ij or server, load these
        // into properties to be merged with app and/or derby props
        Properties ijProps = new Properties();
        Properties srvProps = new Properties();
		if ( (testSpecialProps != null) && (testSpecialProps.length()>0))
		{
		    SpecialFlags.parse(testSpecialProps, ijProps, srvProps);
		}

        /* If there are more than one derby.properties, the rule is to
           load either the test_derby.properties or the default one,
        */
        
        // Check for default_derby.properties
        isClDef = loadTestResource(clDefProp);
		// Check for test specific props 
        isCl = loadTestResource("tests/" + testDirName + "/" + testBase + "_derby.properties");
//System.out.println("**************");
//System.out.println("isCl = " + isCl);
//System.out.println(defaultPackageName + testBase + "_derby.properties");
//System.out.println("**************");
        
        // Now load and merge the properties based on above rules
		if (isCl != null) // In case there exists a test_derby.properties
		{
		    clp.load(isCl);
clp.list(System.out);
		}
		// Try the default_derby.properties instead
		else if (isClDef != null)
		{
		    clp.load(isClDef);
		}

        // j9 will run out of memory with the default cache size (100), so
        // forcing it lower unless set in _derby.properties file for a specific test
        if (jvmName.startsWith("j9"))
        {
            if (clp.getProperty("derby.language.statementCacheSize")==null)
                clp.put("derby.language.statementCacheSize", J9_STATEMENTCACHESIZE);
        }

        // Now merge any special server props if they exist
        // But if clp is still empty, try using the special server props
        if ( clp.isEmpty() )
        {
            // Check for srvProps from testSpecialProps
            if ( !srvProps.isEmpty() )
                clp = srvProps;
        }
        else
        {
            // merge any special properties from testSpecialProps
            if ( !srvProps.isEmpty() )
            {
                for (Enumeration e = srvProps.propertyNames(); e.hasMoreElements();)
                {
                    String key = (String)e.nextElement();
                    String value = srvProps.getProperty(key);
                    if (key.equals("derby.debug.true")) // Add to existing prop
                    {
                        String cval = clp.getProperty("derby.debug.true");
                        // If this property exists, edit to prepend the srvProp
                        // but if the original property is null, just put the srvProp
                        if (cval != null)
                        {
			                if (cval.length() != 0) {
                                // debug property exists, so edit it
                                value = value + "," + cval;
                            } else {
                                // if new debug property is not null, but is zero length, 
                                // assume the intention was to override the debug property.
                                value = "";
                            }
                        }
                    }
                    clp.put(key,value);
                }
            }
        }

        if ( !clp.isEmpty() )
        {
            // Create and load the file

            // This call to getCanonicalPath catches IOExceptions as a workaround to
            // a bug in the EPOC jvm. 
      	    try { clPropFile = new File((new File(baseDir, "derby.properties")).getCanonicalPath()); }
    	    catch (IOException e) {
    		    File f = new File(baseDir, "derby.properties");
    		    FileWriter fw = new FileWriter(f);
    		    fw.close();
    		    clPropFile = new File(f.getCanonicalPath());
    	    }

//System.out.println("clPropFile: " + clPropFile.getPath());
            bos = new BufferedOutputStream(new FileOutputStream(clPropFile));
            clp.store(bos, "Derby Properties");
        	bos.close();
        }

		// --------------------------------- 
        // Check for existence of sed properties file (test_sed.properties)
        // See jdbc_sed.properties
        //  Multiple patterns for DELETE: comma separated
        //    delete=pattern1,pattern2,...,patternn
        //  No commas can be allowed in the patterns.
        // 
        //  Multiple patterns for SUBSTITUTE: comma separated <pattern;substitute> pair
        //    substitute=pattern1;substitute1,pattern2;substitute2,...,patternn;substituten
        //  No commas or semicolons can be allowed in the patterns/subsitutes.  //
       
        if ( testType.equals("multi") )
        	isSed = loadTestResource("multi/stress/" + testBase + "_sed.properties");
        else
        	isSed = loadTestResource("tests/" + testDirName + "/" + testBase + "_sed.properties");
//System.out.println("**************");
//System.out.println("isSed = " + isSed);
//System.out.println(defaultPackageName + testBase + "_sed.properties");
//System.out.println("**************");
		// --------------------------------- 

        // Check for existence of app properties file
        // If there is an test_app, use it to overwrite default_app
        // Then create the -p string for the test
        Properties dp = new Properties();
	    String testPropName = null;
	    String testPropSDName = null; // name for shutdown properties file if needed
	    if (useCommonDB) testPropName = "CDB" + testBase + "_app.properties";
	    else testPropName = testBase + "_app.properties";

        // Check for default_app.properties
        isApDef = loadTestResource(apDefProp);

        // Check for test_app.properties

        if ( testType.equals("multi") )
            isAp = loadTestResource("multi/" + testDirName + "/" + testBase + "_app.properties");
        else
            isAp = loadTestResource("tests/" + testDirName + "/" + testBase + "_app.properties");
//System.out.println("**************");
//System.out.println("isAp = " + isAp);
//System.out.println(defaultPackageName + testBase + "_app.properties");
//System.out.println("**************");


        // Try loading the ap and def properties if they exist
        // Merge only if the test's app properties has usedefaults property
        if ( isAp != null )
        {
            ap.load(isAp);
            // Check for a property usedefaults; if true merge in default props
		    for (Enumeration e = ap.propertyNames(); e.hasMoreElements(); )
		    {
			    String key = (String)e.nextElement();
			    String value = ap.getProperty(key);
			    if ( (key.equals("usedefaults")) && (value.equals("true")) )
			    {
			        // merge in the default properties
                    if ( isApDef != null )
                    {
                        dp.load(isApDef);
                        mergeProps(ap, dp);
                        break;
                    }
                }
            }
        }
        else
        {
            // Just use the default props
            if ( isApDef != null )
                ap.load(isApDef);
        }

        // If app props are still empty, check for any special testSpecialProps
        if ( ap.isEmpty() )
        {
            if ( !ijProps.isEmpty() )
                ap = ijProps;
        }
        else
        {
            // merge any special properties from testSpecialProps
            if ( !ijProps.isEmpty() )
            {
                for (Enumeration e = ijProps.propertyNames(); e.hasMoreElements();)
                {
                    String key = (String)e.nextElement();
                    String value = ijProps.getProperty(key);
                    ap.put(key,value);
                }
            }
        }

        if ( !ap.isEmpty() )
        {
                // Create the file and load the properties
	        // This call to getCanonicalPath catches IOExceptions as a workaround to
	        // a bug in the EPOC jvm. 
            try 
            { 
                appPropFile = new File((new File(baseDir, testPropName)).getCanonicalPath());                 
            }
	        catch (IOException e) 
	        {
		        File f = new File(baseDir, testPropName);
		        FileWriter fw = new FileWriter(f);
		        fw.close();
		        appPropFile = new File(f.getCanonicalPath());		        
	        }

			// For IBM14 the console encoding is different from the platform
			// encoding on windows.  We want it to be the same for our
			// test output like the other JDK's.
			String conEnc = System.getProperty("console.encoding");
			String fileEnc = System.getProperty("file.encoding");
		
			if ((conEnc != null) &&  (fileEnc != null )  &&
				(ap.getProperty("derby.ui.codeset") == null) &&
				conEnc.startsWith("Cp850"))
			{
				ap.put("derby.ui.codeset",fileEnc);
			}

			if (verbose)
				System.out.println("console.encoding:" + conEnc + 
								   " file.encoding:" + fileEnc +
							   " derby.ui.codeset: " + ap.getProperty("derby.ui.codeset"));
			
			// If the initial connection is being specified as a DataSource
			// on the command line using -Dij.dataSource=<dsclassname>
			// then remove the ij.database property that comes from any
			// default_app or other properties file. This is because the
			// ij.database will override the ij.dataSource property.
			if (System.getProperty("ij.dataSource") != null)
			{
				ap.remove("ij.database");
				ap.remove("ij.protocol");
			}
		
//System.out.println("appPropFile: " + appPropFile.getPath());
            bos = new BufferedOutputStream(new FileOutputStream(appPropFile));
            ap.store(bos, "App Properties");
            bos.close();
            
            // Check now through jvmflags for insteresting properties
            // First grab jvmflags from _app.properties for the jvm process cannot
            // use it if just in the test's _app.properties file
            // note that it's already too late if useprocess is false
            String apppropsjvmflags = ap.getProperty("jvmflags");
            if (apppropsjvmflags != null)
            {
                if (jvmflags != null)
                    jvmflags = apppropsjvmflags + "^" + jvmflags;
                else
                    jvmflags = apppropsjvmflags;
            }
            // see if there is anything useful for the test harness in jvmflags
            // from commandline or suite properties
            if ((jvmflags != null) && (jvmflags.length() > 0))
            {
                StringTokenizer st = new StringTokenizer(jvmflags,"^");
                while (st.hasMoreTokens())
                {
                    
                    String tmpstr = st.nextToken();
                    if ((tmpstr.indexOf("=")> 0) && (tmpstr.startsWith("-D")))
                    {
                        // start at position 2, i.e. strip off the "-D"
                        String key = tmpstr.substring(2, tmpstr.indexOf("="));
                        String value = tmpstr.substring((tmpstr.indexOf("=")+1), tmpstr.length());
                        ap.put(key, value);
                    }
                }
            }

            // Depending on the framework, the app prop file may need editing
            if ( (framework.length()>0) || (encryption) )
            {
                try
                {
                    if (!framework.equals("") && 
                        !framework.equals("embedded"))
                        frameworkProtocol(ap);
                    else if (encryption)
                        encryptionProtocol(ap);
                }
                catch(Exception e)
                {
                    System.out.println("Exception: " + e.getMessage());
                    e.printStackTrace();
                }
		
                // write the new properties to the appPropFile
        		appPropFile = new File(baseDir, testBase + "_app.properties");
        		try
        		{
            		bos = new BufferedOutputStream(new FileOutputStream(appPropFile));
            		ap.store(bos, "Test Properties");
            		bos.close();
                }
            	catch(IOException ioe)
            	{
            		System.out.println("IOException creating prop file: " + ioe.getMessage());
            	}
            }
        }

    	if ( (appPropFile != null) && (appPropFile.exists()) )
    	{
    	    // Create the properties string for the test
    		propString = appPropFile.getPath();
    		
    		// Check for shutdown url
    		shutdownurl = ap.getProperty("shutdown");
    		
    		// Check for jdk12test set to true
	        String jdk12test = ap.getProperty("jdk12test");
	        //System.out.println("jdk12test: " + jdk12test);
	        //System.out.println("jvmName: " + jvmName);
	        if (jdk12test != null)
	        {
	            if (jdk12test.toLowerCase().equals("true"))
	            {
	                isjdk12test = true;
	                //System.out.println("isjdk12test " + isjdk12test);
	                if (jvmName.equals("currentjvm")) { // This is not at least jdk12
	                    skiptest = true;
						addSkiptestReason("Test skipped: test needs jdk12, jvm is reporting currentjvm; skipping test: " + scriptFileName);
					}
	            }
	        }

            String srvJvm = System.getProperty("serverJvm");
            if (srvJvm !=null) 
                jvmnet = true;

            String excludeJcc = ap.getProperty("excludeJCC");
            if ( framework.equals("DerbyNet") )
            {	
                try {
                    RunList.checkClientExclusion(excludeJcc, "JCC", jccMajor, jccMinor, javaVersion);
                } catch (Exception e) {
                    skiptest = true;
                    addSkiptestReason(e.getMessage());
                }
            }
		
            // for now we want just want to have a single property
            // for all j9 versions exception j9_foundation
            // which we map to the generic name foundation.
            String testJVM = jvmName;
            if (jvmName.startsWith("j9"))
            {
            	if (jvmName.equals("j9_foundation"))
            		testJVM = "foundation";
            	else
            		testJVM = "j9";
            }
            runwithjvm = ap.getProperty("runwith" + testJVM);
            if  ((runwithjvm != null) && (runwithjvm.equalsIgnoreCase("false")))
            {
				skiptest = true;
				addSkiptestReason("Test skipped: test cannot run with jvm: " +
								  jvmName + ".  " + scriptFileName);
			}
			// startServer will determine whether the server will be started 
			// for network server tests or that will be left to the test.
			String startServerProp = ap.getProperty("startServer");
			if (startServerProp != null &&
				startServerProp.equalsIgnoreCase("false"))
				startServer =false;
			
	        //Check derbyTesting.encoding property
	        if(testEncoding == null) {
	            testEncoding = ap.getProperty("derbyTesting.encoding");
	            // only bother if we have jdk15, otherwise we'll be skipping
	            if ((jvmName.equals("jdk15")) && (testEncoding != null))
	            {
	                    jvmflags = (jvmflags==null?"":jvmflags+" ") 
	                                + "-Dfile.encoding=" + testEncoding; 
	                    ap.put("file.encoding",testEncoding);
	            }
	        }
	       
	        if(!replacePolicyFile) 
	        {
	            String replace_policy = ap.getProperty("derbyTesting.replacePolicyFile");
	            if (replace_policy != null && replace_policy.equals("true"))
	                replacePolicyFile = true;
	            else
	                replacePolicyFile = false;

	        }

 
	        if (NetServer.isJCCConnection(framework)
	        		|| "true".equalsIgnoreCase(ap.getProperty("noSecurityManager")))
	        	runWithoutSecurityManager = true;
	        
   		// Also check for supportfiles
    		String suppFiles = ap.getProperty("supportfiles");
			boolean copySupportFiles = ((suppFiles != null) && (suppFiles.length()>0));
			boolean createExtDirs= new Boolean(ap.getProperty("useextdirs","false")).booleanValue();
    		if (copySupportFiles || createExtDirs)
    		{
				File copyOutDir = null;

    		    if (testType.equals("sql2"))
    		    {
    		        if ( (isSuiteRun) || (framework.length()>0) )
						copyOutDir = outDir;
    		        else if ( (runDir != null) && (runDir.exists()) )
						copyOutDir = runDir;
    		        else
						copyOutDir = outDir;
    		    }
    		    else if ( testType.equals("multi") )
    		    {
    		        if ( (isSuiteRun) || (mtestdir == null) || (mtestdir.length()==0) )
    		        {
						copyOutDir = outDir;
    		        }
    		        else
    		        {
    		            File multiDir = new File(mtestdir);
						copyOutDir = multiDir;
    		        }
    		    }
                else if ( outcopy == true )
                    copyOutDir = outDir;
                else if ( (runDir != null) && (runDir.exists()) )
                {
                    copyOutDir = runDir;
                }
                else
                    copyOutDir = outDir;

				if(createExtDirs)
				{
					extInDir = copyOutDir;
					//create the support file directory for input files
					extInDir = new File(copyOutDir , "extin");
					if(!extInDir.exists())
						extInDir.mkdirs();
					//create the support file directory for output files.
					extOutDir = new File(copyOutDir , "extout");
					if(!extOutDir.exists())
						extOutDir.mkdirs();
					//create the support file directory for input output files.
					extInOutDir = new File(copyOutDir , "extinout");
					if(!extInOutDir.exists())
						extInOutDir.mkdirs();
					copyOutDir = extInDir;
				}
				if(copySupportFiles)
				   CopySuppFiles.copyFiles(copyOutDir, suppFiles);
    		}
    		else
    		{
				// for useprocess false, set ext* (back) to null, or it
				// later tries to delete files even though they don't exist
				extInDir = null;
				extOutDir = null;
				extInOutDir = null;
    		}
    	}
        return propString;
    }

    public static String setTestJavaFlags(String tflags)
    {
        StringBuffer sb = new StringBuffer();
	    if (verbose) System.out.println("testJavaflags: " + tflags);
	    String dintro = "-D";
        try
        {
            dintro = jvm.getJvm(jvmName).getDintro();
        }
        catch (Exception e)
        {
            System.out.println("Problem getting jvm "+jvmName+" Dintro: ");
            e.printStackTrace(System.out);
		    System.exit(1);
        }
	    // Parse because there could be a list of flags
	    StringTokenizer st = new StringTokenizer(tflags,"^");
        while (st.hasMoreTokens())
        {
	        String token = st.nextToken();
            if (! (token.startsWith("-"))) { sb.append(dintro); }
            sb.append(token);
            sb.append(" ");
        }
        if (verbose) System.out.println("setTestJavaFlags returning: " + sb.toString());
        return sb.toString();
    }

	private static void loadProps(Properties p, File f) throws Exception
	{
		if (f.canRead())
		{
			FileInputStream fis = new FileInputStream(f);
			p.load(fis);
			fis.close();
		}
	}

	private static void mergeProps(Properties to, Properties from)
	{
		for (Enumeration e = from.propertyNames(); e.hasMoreElements(); )
		{
			String key = (String)e.nextElement();
			to.put(key, from.getProperty(key));
		}
	}



    private static void frameworkProtocol(Properties p) throws Exception
    {

        if (p == null)
        {
            // No properties
            return;
        }
		for (Enumeration e = p.propertyNames(); e.hasMoreElements(); )
		{
			String key = (String)e.nextElement();
			String value = p.getProperty(key);

			if (key.equals("driver") || key.equals("ij.driver") || key.equals("jdbc.drivers"))
			{
			    p.put(key, driverName);
			}
			else if (key.startsWith("ij.protocol") || key.equals("protocol"))
			{
			    value = NetServer.alterURL(framework,value);
			    p.put(key, value);
			}
			else if (key.equals("database") || key.equals("ij.database") || key.startsWith("ij.connection") || key.equals("jdbc.url"))
			{
			    dbName = value.substring(value.lastIndexOf(':') + 1 ,
						     value.length());
			    value = NetServer.alterURL(framework,value);
			    p.put(key, value);
			}
			else // for any other properties, just copy them
			    p.put(key, value);
			
		}

		
		// jcc default requires userid
		// derby client will default to "APP" so doesn't need harness
		// to set a user.
		if (NetServer.isJCCConnection(framework))
		{
			String user = System.getProperty("ij.user");
			if (user == null) user = "APP";
		    p.put("ij.user",user);
		}

		// both jcc and client require password for the moment
		if (NetServer.isClientConnection(framework))
		{	
			String password = System.getProperty("ij.password");
			if (password == null) password = "APP";
		    p.put("ij.password",password);
		}
		
		if (NetServer.isJCCConnection(framework))
		{
			// force messages to show
			p.put("ij.retrieveMessagesFromServerOnGetMessage","true");	
		}

		// If this is not a known protocol for ij we
		// need to set the driver
		if (driverName != null)
		    p.put("ij.driver",driverName);
	}
	

	private static void encryptionProtocol(Properties p) throws Exception
	{
        String encryptUrl = "dataEncryption=true;bootPassword=Thursday";

        if (p == null)
        {
            // No properties
            return;
        }

        // add encryption algorithm and provider to database creation URL
        String v = p.getProperty("testEncryptionAlgorithm");
        if (v != null)
            encryptUrl += ";" + Attribute.CRYPTO_ALGORITHM + "=" + v;
        v = p.getProperty("testEncryptionProvider");
        if (v != null)
            encryptUrl += ";" + Attribute.CRYPTO_PROVIDER + "=" + v;

		for (Enumeration e = p.propertyNames(); e.hasMoreElements(); )
		{
			String key = (String)e.nextElement();
			String value = p.getProperty(key);

			if (key.equals("database") || key.equals("ij.database") || key.startsWith("ij.connection") || key.equals("jdbc.url"))
			{
				// edit the url if necessary
				int index = value.indexOf(encryptUrl);
				if ( index == -1)
				{
				    value = value + ";" + encryptUrl;
			    }
				p.put(key, value);
			}
            else if (key.equals("testEncryptionAlgorithm") || key.equals("testEncryptionProvider"))
                {} // ignore, do not copy
			else // for any other properties, just copy them
			    p.put(key, value);
		}
	}

    private static void cleanupBaseDir(File baseDir)
    {
        // Some tests rely on no cleanup being done on the baseDir
        boolean okToDelete = false;

        if ( (usesystem == null) || (usesystem.length()==0) )
            okToDelete = true;
        else if (usesystem.equals("nist"))
        {
            if (testBase.equals("schema1"))
                okToDelete = true;
        }
        else if (usesystem.equals("puzzles"))
        {
            if (testBase.equals("puzzleschema"))
                okToDelete = true;
        }

        if (useCommonDB) okToDelete = false;

        if (okToDelete == true)
        {
           //System.out.println("Should be deleting the baseDir for a clean run");
           deleteFile(baseDir);
           if (baseDir.exists())
               System.out.println("baseDir did not get deleted which could cause test failures");
           else
               baseDir.mkdir();
        }
    }

    private static void doCleanup(String javaVersion)
        throws IOException
    {
        boolean status = true;
        // The output files cannot be deleted if there
        // is still a reference to them -- even doing
        // this is not a guarantee that they will be deleted
        // It seems to depend on the Java environment
        //printWriter.close();
        //printWriter = null;

        //Always cleanup the script files - except when keepfiles is true
        if ( !(script == null) && (script.exists()) && (!keepfiles) )
        {
            status = script.delete();
            //System.out.println("Status was: " + status);
        }

	    String endTime = CurrentTime.getTime();
	    StringBuffer sbend = new StringBuffer();
	    sbend.append("*** End:   " + testBase + " jdk" + javaVersion + " ");
	    if ( (framework.length()>0) && (!framework.startsWith("embedded")) )
	        sbend.append(framework + " ");
	    if ( (suiteName != null) && (suiteName.length()>0) )
	        sbend.append(suiteName + " ");
	    sbend.append(endTime + " ***");
	    System.out.println(sbend.toString());
	    pwDiff.println(sbend.toString());
        pwDiff.flush();
        pwDiff.close();
        pwDiff = null;

        // This could be true if set by user or there were diffs
        if (keepfiles == false)
        {
            // Delete the out and diff files
            status = tmpOutFile.delete();
            if (status == false)
                tmpOutFile = null;
            status = finalOutFile.delete();
            if (skiptest == false)
                status = diffFile.delete();           
            // delete the copied (and for Network Server, modified) master file 
            tempMasterFile = new File(outDir, tempMasterName);
            status = tempMasterFile.delete();
            UTF8OutFile = new File(outDir, UTF8OutName);
            status = UTF8OutFile.delete();
            File defaultPolicyFile = new File(userdir, "derby_tests.policy");
            if (defaultPolicyFile.exists())
            status = defaultPolicyFile.delete();
            if (deleteBaseDir)
            {
                if (useCommonDB == false) 
                {
                	//System.out.println("Should delete the baseDir: " + baseDir.getPath());
                	deleteFile(baseDir);
		        }
		        else 
		        {
			        status = appPropFile.delete();
			        //no permission in Java to drop derby.log
			        //File logfile = new File(baseDir, "derby.log");
			        //System.out.println("delete derby.log ");
			        //System.out.println(logfile.getPath());
			        //status = logfile.delete();
		        }

                // delete the directories where external input/output files 
                // were created
                if (extInDir!=null) deleteFile(extInDir);
                if (extOutDir!=null) deleteFile(extOutDir);
                if (extInOutDir!=null) deleteFile(extInOutDir);
            }
        }
	    // reset for next test
	    // the next line is a bug fix to get cleanup working correctly when
	    // useprocess==false;  without this the first failing test causes all
	    // subsequent tests not to clean up, since keepfiles is static; a
	    // more general solution is to make the variable non-static, and to
	    // set the default in the initializer; I(john) have not done this
	    // because the same reasoning probably applies to many of the static
	    // variables, and the whole subject should probably be approached in
	    // a coordinated way when someone has the time for it.
	    keepfiles = false;

    }

    /*
     * For recursively deleting a directory
     *
     */
	public static void deleteFile(File f)
	{
	    boolean status = true;
        if (f == null)
        {
            System.out.println(f.getName() + " is null");
            return;
        }
        if (!f.exists())
        {
            System.out.println(f.getName() + " does not exist; harness error");
            return;
        }

        //System.out.println("Trying to delete: " + f.getPath());
	    status = f.delete();

	    if (status == true)
	        return;
	    else
	    {
	        // Could not delete; this could be a non-empty directory
	        if (!f.isDirectory())
	        { 
	            System.out.println("Could not delete file " + f.getName() + ", going on");
	            return;
	        } 
	        //System.out.println("Recursively delete...");
	        String[] files = f.list();
	        for (int i = 0; i < files.length; i++)
	        {
	            File sub = new File(f.getPath(), files[i]);
	            if (sub == null)
	                continue;
	            //System.out.println("Try to delete: " + sub.getPath());
	            status = sub.delete();
	            if (status != true)
	            {
	                // Could not delete; it may be a non-empty dir
	                if ( sub.isDirectory() )
	                {
	                    //System.out.println("Recursing again ... ");
	                    deleteFile(sub);
	                    // It should be empty now; try again
	                    status = sub.delete();
	                    //System.out.println("Recurse delete status: " + status);
	                }
	                //else
	                // The file delete failed
	                //System.out.println("Failed to clean up file: " + sub.getPath());
	            }
	        }
	    }
	    //Should be able to delete the top directory now
	    status = f.delete();
	    if (status == false)
	    {
	        System.out.println("Warning: Cleanup failed on baseDir: " + f.getPath());
	    }
	}

	static void addToFailures(String testName) throws IOException
	{
		if (failFileName==null)
		{
			if (isSuiteRun) failFileName = suiteName+".fail";
			else failFileName=testBase+".fail";
        }
		addToListFile(failFileName,testName);
	}

	static void addToSuccesses(String testName) throws IOException
	{
		if (passFileName==null)
		{
			if (isSuiteRun) passFileName = suiteName+".pass";
			else passFileName=testBase+".pass";
        }
		addToListFile(passFileName,testName);
    }

	static void addToListFile(String fileName, String testName) throws IOException
	{
	    File f;
	    if (isSuiteRun)
	        f = new File(rsuiteDir,fileName);
	    else
	        f = new File(outDir,fileName);
		PrintStream ps = null;

		// This call to getCanonicalPath catches IOExceptions as a workaround to
		// a bug in the EPOC jvm. 
		try { ps = new PrintStream( new FileOutputStream(f.getCanonicalPath(),true) ); }
		catch (IOException e) {
		    FileWriter fw = new FileWriter(f);
		    fw.close();
		    ps = new PrintStream( new FileOutputStream(f.getCanonicalPath(),true) ); 
		}

		ps.println(testName);
		ps.flush();
		ps.close();
    }

    static void appendStderr(BufferedOutputStream bos, InputStream is)
        throws IOException
    {
        PrintWriter tmpPw = new PrintWriter(bos);
        // reader for stderr
        BufferedReader errReader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        String s = null;
        int lines = 0;
        while ((s = errReader.readLine()) != null)
        {
           tmpPw.println(s);
        }
        errReader.close();
        tmpPw.flush();
    }

    static void verifyTestType()
        throws ClassNotFoundException, FileNotFoundException, IOException
    {
        //java requires '/' as separator to look into jar, irrespective of OS
        InputStream is =
            loadTestResource("harness/testtypes.properties");
		Properties p = new Properties();
		p.load(is);
        String testtypes = p.getProperty("testtypes");
	    StringTokenizer st = new StringTokenizer(testtypes,",");
	    String ttype = "";
        while (st.hasMoreTokens())
        {
            ttype = st.nextToken();
            if ( testType.equals(ttype) )
                return;
        }
        // Not a known test type
        System.out.println("Unknown test type: " + testType);
        System.exit(1);
    }

	public static void
	addStandardTestJvmProps(Vector testJvmProps,String derbySystemHome,
							String userDirName, jvm jvm)
	{
		if (derbySystemHome==null || derbySystemHome.length() == 0)
			derbySystemHome = userDirName;
		testJvmProps.addElement("derby.system.home=" + derbySystemHome);
		testJvmProps.addElement("derby.infolog.append=true ");
		// Why is this being done here
		//if (jvm != null)
		    //testJvmProps.addElement("jvm="+jvm.getName());
	}
	
	private static String[] buildTestCommand(String propString,
	    String systemHome, String scriptPath)
	    throws FileNotFoundException, IOException, Exception
	{
    	composePolicyFile();
        
	    //System.out.println("testType: " + testType);
	    String ij = "";
        // Create the test command line
        if (testType.equals("sql"))
            ij = "ij";
		jvm jvm = null; // to quiet compiler
		jvm = jvm.getJvm(jvmName);
        if (javaCmd != null)
            jvm.setJavaCmd(javaCmd);

        if ( (classpath != null) && (classpath.length()>0) )
            jvm.setClasspath(classpath);

		Vector jvmProps = new Vector();
		if ( testType.equals("java") || testType.equals("demo") )
		    addStandardTestJvmProps(jvmProps,systemHome,
			    outDir.getCanonicalPath(),null);		    
        else if ( (runDir != null) && (runDir.exists()) )
		    addStandardTestJvmProps(jvmProps,systemHome,
			    runDir.getCanonicalPath(),jvm);
        else
			addStandardTestJvmProps(jvmProps,systemHome,
				outDir.getCanonicalPath(),jvm);
		
        if ( (testJavaFlags != null) && (testJavaFlags.length()>0) )
        {
	    String parsedFlags = setTestJavaFlags(testJavaFlags);
            StringTokenizer st = new StringTokenizer(parsedFlags," ");
            while (st.hasMoreTokens())
            {
                jvmflags = (jvmflags==null?"":jvmflags) + " " + st.nextToken();
            }
        }
        
        if ( ij.startsWith("ij") )
            jvmProps.addElement("ij.defaultResourcePackage=" +
                defaultPackageName);
        
        if ( (framework != null) )
        {
            jvmProps.addElement("framework=" + framework);
            if ((hostName != null) && (!hostName.equals("localhost")))
            		jvmProps.addElement("hostName=" + hostName);
        }
        
        if (junitXASingle)
            jvmProps.addElement ("derbyTesting.xa.single=true");

        // if we're not jdk15, don't, we'll skip
        if ((testEncoding != null) && (jvmName.equals("jdk15")))
        {
            jvmProps.addElement("derbyTesting.encoding=" + testEncoding);
            jvmProps.addElement("file.encoding=" + testEncoding);
            jvmflags = (jvmflags==null?"":jvmflags+" ") 
                         + "-Dfile.encoding=" + testEncoding; 
        }
        
        if (upgradejarpath != null)
            jvmProps.addElement("derbyTesting.jar.path=" + upgradejarpath);
            
        if ( (jvmflags != null) && (jvmflags.length()>0) )
        {
            // We now replace any '^' in jvmflags with ' '
            if (jvmflags.indexOf("^")>0)
            {
                jvmflags = spaceJvmFlags(jvmflags);
            }
            jvm.setFlags(jvmflags);
        }
        
        
        if (testType.equals("multi"))
        {
            if ( (jvmflags != null) && (jvmflags.indexOf("mx") == -1) && (jvmflags.indexOf("Xmx") == -1))
                jvm.setMx(64*1024*1024); // -mx64m
            
            // MultiTest is special case, so pass on properties
            // related to encryption to MultiTest
            jvmProps.addElement("encryption="+encryption);
            Properties props = new Properties();
            // parse and get only the special properties that are needed for the url 
            SpecialFlags.parse(testSpecialProps, props, new Properties());
            String encryptionAlgorithm = props.getProperty("testEncryptionAlgorithm");
            if(encryptionAlgorithm != null)
                jvmProps.addElement("encryptionAlgorithm=\""+ Attribute.CRYPTO_ALGORITHM 
                        +"="+encryptionAlgorithm+"\"");
        }
        jvm.setD(jvmProps);
       
        // set security properties
        if (!runWithoutSecurityManager)
            jvm.setSecurityProps();
        else
        	System.out.println("-- SecurityManager not installed --");
            
        Vector v = jvm.getCommandLine();
        if ( ij.startsWith("ij") )
        {
            // As of cn1411-20030930 IBM jvm the system takes the default
            // console encoding which in the US, on windows, is Cp437.
            // Sun jvms, however, always force a console encoding of 1252.
            // To get the same result for ibm141 & jdk14*, the harness needs to
            // force the console encoding to Cp1252 for ij tests - unless 
            // we're on non-ascii systems.
            String isNotAscii = System.getProperty("platform.notASCII");
            if ( (isNotAscii == null) || (isNotAscii.equals("false"))) 
                v.addElement("-Dconsole.encoding=Cp1252" );
            v.addElement("org.apache.derby.tools." + ij);
            if (ij.equals("ij"))
            {
                //TODO: is there a setting/property we could check after which
            	// we can use v.addElement("-fr"); (read from the classpath?)
                // then we can also use v.addElement(scriptFile);
            	v.addElement("-f");
                v.addElement(outDir.toString() + File.separatorChar + scriptFileName);
            }
            v.addElement("-p");
            v.addElement(propString);
        }
        else if ( testType.equals("java") )
        {
            if (javaPath.length() > 0)
                v.addElement(javaPath + "." + testBase);
            else
                v.addElement(testBase);
            if (propString.length() > 0)
            {
                v.addElement("-p");
                v.addElement(propString);
            }
        }
        else if (testType.equals("unit"))
        {
            v.addElement("org.apache.derbyTesting.unitTests.harness.UnitTestMain");
            v.addElement("-p");
            v.addElement(propString);
        }
        else if (testType.equals("junit"))
        {
            v.addElement("junit.textui.TestRunner");
            if (javaPath.length() > 0) {
                v.addElement(javaPath + "." + testBase);
            } else {
                v.addElement(testBase);
            }
        }
        else if ( testType.equals("multi") )
        {
	System.out.println("scriptiflename is: " + scriptFileName);
            v.addElement("org.apache.derbyTesting.functionTests.harness.MultiTest");
            v.addElement(scriptFileName);
            v.addElement("-i");
            v.addElement(mtestdir);
            v.addElement("-o");
            v.addElement(outDir.getPath());
            v.addElement("-p");
            v.addElement(propString);
        }

        // Now convert the vector into a string array
        String[] sCmd = new String[v.size()];
        for (int i = 0; i < v.size(); i++)
        {
            sCmd[i] = (String)v.elementAt(i);
        }

        return sCmd;
    }

    public static String spaceJvmFlags(String caretedJvmFlags)
    {
    	String spacedJvmFlags = "";
    	// there must at least be one
    	StringTokenizer st = new StringTokenizer(jvmflags,"^");
    	while (st.hasMoreTokens())
    	{
    	    spacedJvmFlags += st.nextToken() + " ";
    	}
    	spacedJvmFlags = spacedJvmFlags.substring(0,spacedJvmFlags.length() -1);
    	return spacedJvmFlags;    
    }
    
    public static void composePolicyFile() throws ClassNotFoundException
    {
        try{
            //DERBY-892: allow for test- and suite-specific policy additions
            
            // this is the default policy file
            String default_policy = "util/derby_tests.policy";
            
            // if the property replacePolicyFile is set (in the 
            // test specific _app.properties file, or at the command line)
            // do not use the default policy file at all, otherwise, append
            if (!replacePolicyFile)
            {
            	File userDirHandle = new File(userdir);
            	CopySuppFiles.copyFiles(userDirHandle, default_policy);
            }
            // see if there is a suite specific policy file and append or replace
            if ((isSuiteRun) && (suiteName!=null)) 
            {
                InputStream newpolicy = loadTestResource("suites/" + 
                    suiteName.substring(0,suiteName.indexOf(':')) + 
                    ".policy");
                writePolicyFile(newpolicy);
            }

            // if we are running with useprocess=false, we need some special
            // properties (setSecurityManager, setIO)
            if (!useprocess) 
            {
                InputStream newpolicy = loadTestResource("util/" + "useprocessfalse.policy");
                writePolicyFile(newpolicy);
            }
            
            // now get the test specific policy file and append or replace
            InputStream newpolicy =
                loadTestResource("tests/" + testDirName + "/" + testBase + ".policy");
            writePolicyFile(newpolicy);
        } catch (IOException ie) {
            System.out.println("Exception trying to create policy file: ");
            ie.printStackTrace(); 
        }
    }

    public static void writePolicyFile(InputStream newpolicy)
    {
        try{
            if (newpolicy != null)
            {
                File oldpolicy = new File(runDir,"derby_tests.policy");
                if (verbose && oldpolicy.exists()) System.out.println("Appending to derby_tests.policy");
                BufferedReader policyadd = new BufferedReader(new InputStreamReader(newpolicy, "UTF-8"));
                FileWriter policyfw = new FileWriter(oldpolicy.getPath(), true);
                PrintWriter policypw = new PrintWriter( new BufferedWriter(policyfw, 10000), true );
                String str = "";
                while ( (str = policyadd.readLine()) != null ) { policypw.println(str); }
                policypw.close();
                policyadd.close();
                policypw= null;
                newpolicy = null;
            }
        } catch (IOException ie) {
            System.out.println("Exception trying to create policy file: ");
            ie.printStackTrace(); 
        }
    }

    private static void execTestProcess(String[] testCmd)
        throws Exception
    {
        // Execute the process and handle the results
    	Process pr = null;
    	try
    	{
            // We need the process inputstream and errorstream
            ProcessStreamResult prout = null;
            ProcessStreamResult prerr = null;
            FileOutputStream fos = null;
            BufferedOutputStream bos = null;
            
            StringBuffer sb = new StringBuffer();
            
            for (int i = 0; i < testCmd.length; i++)
            {
                sb.append(testCmd[i] + " ");                    
            }
            if (verbose) System.out.println(sb.toString());
            pr = Runtime.getRuntime().exec(testCmd);

            if (useOutput)
            {
                fos = new FileOutputStream(tmpOutFile);
                bos = new BufferedOutputStream(fos, 1024);
                prout = 
                    new ProcessStreamResult(pr.getInputStream(), bos, 
                    					timeoutStr, testEncoding);
            }
            else
            {
                fos = new FileOutputStream(stdOutFile);
                bos = new BufferedOutputStream(fos, 1024);
                prout = 
                    new ProcessStreamResult(pr.getInputStream(), bos, 
                    					timeoutStr, testEncoding);
            }
            prerr =
                new ProcessStreamResult(pr.getErrorStream(), bos, 
                						timeoutStr, testEncoding);
    
            if (framework != null && ! framework.equals(""))
                if (verbose) System.out.println("The test should be running...");

			if (timeout != -1) {
				TimedProcess tp = new TimedProcess(pr);
				tp.waitFor(timeout*60);
				pr = null;
			}

            // determine if the process is done or was interrupted
            boolean outInterrupt = prout.Wait();
            boolean errInterrupt = prerr.Wait();                
                
            if ( (errInterrupt) || (outInterrupt) )
            {
                pwDiff.println("The test timed out...");
                System.out.println("Test timed out...");
                pr.destroy();
                pr = null;
            }

            fos.close();
            bos.close();
            //printWriter.flush();
            //printWriter.close();
            if (pr != null)
            {
                pr.destroy();
                pr = null;
            }
        }
        catch(IOException ioe)
        {
            System.out.println("IOException: " + ioe.getMessage());
        }
        catch(Throwable t)
        {
            pwDiff.println("Process exception: " + t);
            System.out.println("Process exception: " + t.getMessage());
            t.printStackTrace();
            if (pr != null)
            {
                pr.destroy();
                pr = null;
            }
        }
    }
    
    private static void execTestNoProcess(Properties sysProp,
        String systemHome, String propString, String scriptPath)
        throws Exception
    {
        // For platforms where executing a process is failing
        Properties ptmp = System.getProperties();
        ptmp.put("derby.system.home", systemHome);
        ptmp.put("derby.infolog.append", "true");
        // for framework tests, we may need to pick up the hostName 
        // passed on on command line to individual tests...
        if (framework.startsWith("DerbyNet"))
        	ptmp.put("hostName=", hostName);
        System.setProperties(ptmp);

	    String pathStr = "";

	    //these calls to getCanonicalPath catch IOExceptions as a workaround to
	    //a bug in the EPOC jvm. 
	    try 
	    { 
	        pathStr = tmpOutFile.getCanonicalPath().replace(File.separatorChar,fileSep); 
	    }
	    catch (IOException e) 
	    {
	        FileWriter fw = new FileWriter(tmpOutFile);
	        fw.close();
	        pathStr = tmpOutFile.getCanonicalPath().replace(File.separatorChar,fileSep);
	    }
	    

    	PrintStream ps = new PrintStream(new FileOutputStream(pathStr), true);
    	
    	// Install a security manager within this JVM for this test.
    	composePolicyFile();
    	boolean installedSecurityManager = installSecurityManager();
    	if (testType.equals("sql"))
    	{
    	    String[] ijarg = new String[3];
            ijarg[0] = "-p";
            ijarg[1] = propString;
            ijarg[2] = scriptPath;
            // direct the standard output and error to the print stream for the .tmp file
            PrintStream stdout = System.out;
            PrintStream stderr = System.err;
            System.setOut(ps);
            System.setErr(ps);
			RunIJ ij = new RunIJ(ijarg);
			Thread ijThread = new Thread(ij);
			try
			{
				ijThread.start();
				if (timeout < 0)
				{
					ijThread.join();
				}
				else
				{
				    ijThread.join(timeout * 60 * 1000);
				}
			}
			catch (Exception e)
			{
				System.out.println("Aiiie! Got some kind of exception " + e);
			}

			// Now make sure a shutdown is complete if necessary
			if (shutdownurl != null)
			{
			    String[] sdargs = new String[2];
			    sdargs[0] = systemHome;
			    sdargs[1] = shutdownurl;
			    shutdown.main(sdargs);
			}
			// Reset ij.defaultResourcePackage
			ptmp = System.getProperties();
                        ptmp.put("ij.defaultResourcePackage", "/org/apache/derbyTesting/");
			ptmp.put("usesystem", "");
			System.setProperties(ptmp);
			// Reset System.out and System.err
			System.setOut(stdout);
			System.setErr(stderr);
        }
        else if (testType.equals("java"))
        {
	    if (javaPath == null)
	            javaPath = "org.apache.derbyTesting.functionTests.tests." + testDirName;
	    
            String[] args = new String[2];
            args[0] = "-p";
            args[1] = propString;
            Class[] classArray = new Class[1];
            classArray[0] = args.getClass();
            String testName = javaPath + "." + testBase;
            Class JavaTest = Class.forName(testName);
            PrintStream stdout = System.out;
            PrintStream stderr = System.err;
            System.setOut(ps);
            System.setErr(ps);
            // Get the tests's main method and invoke it
            Method testMain = JavaTest.getMethod("main", classArray);
            Object[] argObj = new Object[1];
            argObj[0] = args;
			RunClass testObject = new RunClass(testMain, argObj);
			Thread testThread = new Thread(testObject);
			try
			{
				testThread.start();
				if (timeout < 0)
				{
					testThread.join();
				}
				else
				{
					testThread.join(timeout * 60 * 1000);
				}
			}
			catch(Exception e)
			{
				System.out.println("Exception upon invoking test..." + e);
				e.printStackTrace();
			}

			try 
			{
				java.sql.DriverManager.getConnection("jdbc:derby:;shutdown=true");
			} 
			catch (java.sql.SQLException e) 
			{
				// ignore the errors, they are expected.
			}
	        // Reset System.out and System.err
	        System.setOut(stdout);
	        System.setErr(stderr);
        }
        else if (testType.equals("multi"))
        {
	System.out.println("scriptiflename is: later " + scriptFileName);
            // multi tests will now run with useprocess=false;
            // however, probably because they use Threads, if run 
            // within another test suite, the next suite will not run
            // And using a Thread.join() to start the tests doesn't resolve
            // this. So this support is here simply to allow running
            // something like stressmulti just by itself for debugging
            //javaPath = "org.apache.derbyTesting.functionTests.harness.MultiTest";
            String[] args = new String[5];
            args[0] = scriptFileName;
            args[1] = "-i";
            args[2] = mtestdir;
            args[3] = "-o";
            args[4] = outDir.getPath();
            System.out.println("Try running MultiTest.main");
            for (int i = 0; i < args.length; i++)
                System.out.println("args: " + args[i]);
            System.exit(1);
            org.apache.derbyTesting.functionTests.harness.MultiTest.main(args);
        }
        else if (testType.equals("unit"))
        {
            // direct the standard output and error to the print stream for the .tmp file
            PrintStream stdout = System.out;
            PrintStream stderr = System.err;
            System.setOut(ps);
            System.setErr(ps);
            System.out.println("Unit tests not implemented yet with useprocess=false");
            // Reset System.out and System.err
            System.setOut(stdout);
            System.setErr(stderr);
	        // repeat to stdout...
	        System.out.println("Unit tests not implemented yet with useprocess=false");
            //System.exit(1);
            /*
            String[] args = new String[2];
            args[0] = "-p";
            args[1] = propString;
            org.apache.derbyTesting.unitTests.harness.UnitTestMain.main(args);
            */
        }
        else if (testType.equals("junit"))
        {
            PrintStream stdout = System.out;
            PrintStream stderr = System.err;
            System.setOut(ps);
            System.setErr(ps);            
            if (javaPath == null) {
                javaPath = "org.apache.derbyTesting.functionTests.tests." + testDirName;
            }
            String testName = javaPath + "." + testBase;
            
            // NOTE: This is most likely a temporary solution that will be 
            //       replaced once we get our own TestRunner.
            
            // Cannot use junit.textui.TestRunner.main() since it will exit 
            // the JVM when done.
            
            // Extract/create a TestSuite object, which is either
            //      a) retreived from a static suite() method in the test class
            //
            // or, if a) fails
            //
            //      b) containing all methods starting with "test" in the junit 
            //         test class.
            // This corresponds to what the junit.textui.TestRunner would
            // do if invoked in a separate process (useprocess=true).
           
            // Load the test class
            Class testClass = Class.forName(testName);
            
            TestSuite junitTestSuite = null;
            
            try{
                // Get the static suite() method if it exists.
                Method suiteMethod = testClass.getMethod("suite", null);
                // Get the TestSuite object returned by the suite() method
                // by invoking it.
                // Method is static, hence param1 is null
                // Method has no formal parameters, hence param2 is null
                junitTestSuite = (TestSuite) suiteMethod.invoke(null, null);
            } catch(Exception ex){
                // Not able to access static suite() method (with no params)
                // returning a TestSuite object.
                // Use JUnit to create a TestSuite with all methods in the test
                // class starting with "test"
                junitTestSuite = new TestSuite(testClass);
            }
            
            if(junitTestSuite != null){
                // Now run the test suite
                junit.textui.TestRunner.run(junitTestSuite);    
            }
            else{
                System.out.println("Not able to extract JUnit TestSuite from " +
                        "test class " + testName);
            }
        
            // Reset System.out and System.err
            System.setOut(stdout);
            System.setErr(stderr);
        }        
        ps.close();
         if (installedSecurityManager)
        {
        	System.setSecurityManager(null);
        	
        }        
    }

    static void addSkiptestReason(String reason) {
		if (skiptestReason.length() > 0)
			skiptestReason.append(System.getProperty("line.separator","\n"));
		skiptestReason.append(reason);
    }

    static boolean verifyFramework (String framework) {
    	String validFmString = "";
    	for (int i = 0 ; i < validFrameworks.length; i++)
    	{
    	    validFmString   += " " + validFrameworks[i];
    	    if (validFrameworks[i].equals(framework))
        		return true;
	    }
    	System.err.println("Invalid framework: " + framework);
	
    	System.err.println("Valid frameworks are: " + 
			   validFmString);
    	return false;
	
    }

    /*
     * method for loading a resource relative to testResourceHome.
     * @param loc location of file relative to testResourceHome (e.g. master/cast.out)
     * @return InputStream for the resource
     */
    public static InputStream loadTestResource(String loc) {
        return new Object().getClass().getResourceAsStream(testResourceHome + loc); 
    }
    
    /*
     * This method replaces the / in the path string with \ for windows
     * @param path the path string to convert
     * @return String the new path
     */
    private static String convertPathForWin(String path)
    {
        String tmp = "a" + path +"a";
        int i = tmp.indexOf('/');
        while (i != -1)
        {
            //replace the \\ with .
            tmp = tmp.substring(0, i) + "\\\\" + tmp.substring(i+1);
            i = tmp.indexOf('/');
        }
        //lets remove the a we added
        tmp = tmp.substring(1, tmp.length()-1);
        //System.out.println(tmp);
        
        return tmp;
    }
    
    /**
     * Install the default security manager in this JVM for this
     * test, used when useprocess is false.
     * @return
     * @throws ClassNotFoundException
     * @throws IOException
     */
    private static boolean installSecurityManager() throws ClassNotFoundException, IOException
    {
    	// SecurityManager not currently work with j9 and useProcess=false
    	// need to disable to allow tests to run.
    	if (jvmName.startsWith("j9"))
    		return false;
    	
    	boolean installedSecurityManager = false;
    	// Set up the SecurityManager in this JVM for this test.
    	boolean haveSecurityManagerAlready = System.getSecurityManager() != null;
        if (runWithoutSecurityManager)
        {
        	// Test doesn't run with a SecurityManager,
        	// print a warning if one is there already.
        	if (haveSecurityManagerAlready)
        		System.out.println(
        				"noSecurityManager=true,useProcess=false but SecurityManager installed by previous test");
        	else
        	    System.out.println("-- SecurityManager not installed --");
        }     
        else if (!haveSecurityManagerAlready)
    	{
        	// Get the set of -D options that would be needed
        	// for a spawned VM and convert them to system properties.
    	    Vector propList = jvm.getSecurityProps(null);
    	    for (Enumeration e = propList.elements(); e.hasMoreElements();)
    	    {
    	    	String dashDOpt = (String) e.nextElement();
    	    	if ("java.security.manager".equals(dashDOpt))
    	    		continue;
    	    	
    	    	int eq = dashDOpt.indexOf("=");
    	    	String key = dashDOpt.substring(0, eq);
    	    	String value = dashDOpt.substring(eq + 1);
    	    	
    	    	System.setProperty(key, value);
    	    	
     	    }
		    System.setSecurityManager(new SecurityManager());
		    installedSecurityManager = true;
    	}
        
        return installedSecurityManager;
    }
    
    // copy the .out file in utf8 format. 
    // This can then be used as a new master in the source.
    // It is assumed that if one runs with this property, keepfiles should be true.
    private static void generateUTF8OutFile(File FinalOutFile) throws IOException
    {
        if (generateUTF8Out) 
        {
            keepfiles=true;
        	File UTF8OutFile = new File(UTF8OutName);
        	
        	// start reading the .out file back in, using default encoding
        	BufferedReader inFile = new BufferedReader(new FileReader(FinalOutFile));
        	FileOutputStream fos = new FileOutputStream(UTF8OutFile);
        	BufferedWriter bw = new BufferedWriter (new OutputStreamWriter(fos, "UTF-8"));  
        	int c;
        	while ((c = inFile.read()) != -1)
        		bw.write(c);
        	bw.flush();
        	bw.close();
        	fos.close();     
        }
    }

}


