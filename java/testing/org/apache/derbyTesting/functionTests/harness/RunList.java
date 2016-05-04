/* 

   Derby - Class org.apache.derbyTesting.functionTests.harness.RunList

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

//import org.apache.derby.tools.sysinfo;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.PrintStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.lang.ClassNotFoundException;
import java.lang.reflect.Method;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Vector;
import java.util.StringTokenizer;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.derbyTesting.functionTests.util.TestUtil;


public class RunList
{

	static String jvmName = "currentjvm";
	static String javaCmd = "java";
	static String javaArgs;
	static jvm jvm;
	static String javaVersion; // System.getProperty("java.version")
	static String majorVersion;
	static String minorVersion;
	static String jversion; // to pass jvm to RunTest as -Djvm=1.2 etc.
	static int iminor;
	static int imajor;
	static boolean skip = false;
	static boolean driverNotFound = false;
	static boolean needSync = false;
	static boolean needJdk12 = false;
	static boolean needJdk12ext = false;
	static boolean excludedFromJCC = false;
	static String clientExclusionMessage;
	static Boolean needIBMjvm = null;
	static boolean needEncryption = false;
	static String jvmflags;
	static String testJavaFlags;
	static String classpath;
	static String classpathServer;
    static String framework;
    static String usesystem;
    static String upgradetest;
    static String jarfile;
    static String useoutput;
	static String keepfiles = "false";
	static String encryption;
	static String testEncryptionProvider;
	static String testEncryptionAlgorithm;
	static String jdk12test;
	static String jdk12exttest;
	static String runwithibmjvm = null;
	static String runwithj9;
	static String runwithjvm;
	static String excludeJCC;
	static boolean useprocess = true;
	static String skipsed = "false";
	static boolean fw_set = false;
	static String systemdiff = "false";
	static String suiteName = "";
	static String fullsuiteName = "";
	static String topSuiteName = ""; // The very top suite creating RunLists
	static String topParentSuite = ""; // The "subparent" of the very top suite
	static String topSpecialProps = ""; // special properties at the top suite
	static String otherSpecialProps = ""; // special properties (individual suite)
	static String ijdefaultResourcePackage; // for ij tests, the package resource
	static String outcopy; // cases where copyfiles should go to outDir
	static String userdir; // current user directory
	static String mtestdir; // required by multi tests
	static boolean verbose = false; // for debug output
	static String reportstderr;
	static String timeout;
	static String shutdownurl;
    static PrintWriter pwOut; // for writing suite output
	static String outputdir; // location of output (default is userdir)
	static String topsuitedir; // for nested suites, need top output location
	static String topreportdir; // where to place the .pass and .fail files
	static String canondir; // location of masters (default is master)
	static String bootcp; // for j9 bootclasspath
	static String serverJvm; // for starting another jvm for networkserver, j9 default.
	static String serverJvmName; // for starting another jvm for networkserver, j9_22 default.
    static File outDir; // test out dir
    static File outFile; // suite output file
    static File runDir; // location of suite.runall (list of tests)
	static File runFile; // suite.runall file
	static Properties suiteProperties;
	static Properties specialProperties; // for testSpecialProps
	static BufferedReader runlistFile;
	static String hostName;
	static String testEncoding;	// Encoding used for child jvm and to read the test output 
	static String upgradejarpath;	// Encoding used for child jvm and to read the test output 
        static String derbyTestingXaSingle;// Run junit test cases with under 
                                           // single branck xa transaction
	
    static String [] clientExclusionKeywords = new String [] {
        "at-or-before:", "at-or-after:", "when-at-or-before:jdk",
        "when-at-or-after:jdk", "when:jdk"
    };

    public RunList()
    {
    }

    /**
    * RunList
    * suitesToRun: a Vector of suites from RunSuite
    * outDir: The output directory for the suite(s)
    * pwOut: The output for writing suite and test results
    * suiteProperties: From RunSuite for the top suite
    * (individual suites in the vector may have their own
    * properties which must also be located and applied)
    */

    public RunList(Vector suitesToRun, 
        File runDir, File outDir, PrintWriter pwOut,
        Properties suiteProperties, 
        Properties specialProperties, 
        String topParentSuite)
        throws ClassNotFoundException, IOException, Exception
    {
        this.runDir = runDir;
        this.outDir = outDir;
        this.pwOut = pwOut;
        this.suiteProperties = suiteProperties; // usual suite props
        this.specialProperties = specialProperties; // for special test Flags
        this.topSuiteName = suiteProperties.getProperty("suitename");
        //System.out.println("----------------------------------------");
        //System.out.println("RunList topSuiteName= " + topSuiteName);
        this.topParentSuite = topParentSuite;
        //System.out.println("topParentSuite= " + topParentSuite);

        // Run the suites
        runSuites(suitesToRun);
    }

    private static void runSuites(Vector suitesToRun)
        throws ClassNotFoundException,
        FileNotFoundException, IOException, Exception
    {
        // For each suite, locate its properties and runall files
        // which should be in the "suites" dir or user.dir
        String suiteName = "";
        userdir = System.getProperty("user.dir");
        //System.out.println("Number of suites in list = " + suitesToRun.size());
        Properties p = null;

        // First get the top level suiteProperties since some
        // special properties might need to be used by all sub-suites
        setTopSuiteProperties();
        // Now set the properties for the topParentSuite (a sub-parent of the top)
        Properties topParentSuiteProps = 
            locateSuiteProperties(topParentSuite, suiteProperties, true, true);
        setSuiteProperties(topParentSuiteProps, topParentSuite, suiteProperties, true, true);

        // Now handle the list of child suites under this parent
        for (int i = 0; i < suitesToRun.size(); i++)
        {
            /* Note: nesting of suites can be complex, especially if the
             subsuites of the top suite also contain subsuites; we must take
             care in setting of special properties like framework which may
             need to propagate to its subsuites, but not back to the very top
            */
            Properties subProps = null;
            fullsuiteName = (String)suitesToRun.elementAt(i);
            //System.out.println("fullsuiteName: " + fullsuiteName);
            String subSuite = fullsuiteName.substring(0,fullsuiteName.lastIndexOf(":"));
            //System.out.println("subSuite: " + subSuite);
            if ( !subSuite.equals(topParentSuite) )
            {
                subProps = locateSuiteProperties(subSuite, topParentSuiteProps, true, false);
            }
            else
            {
                // reset in case a previous subsuite had set framework, etc
                subProps = topParentSuiteProps;
            }
            setSuiteProperties(subProps, subSuite, topParentSuiteProps, true, false);

            // Now handle the child suite of this subSuite
            suiteName = fullsuiteName.substring(fullsuiteName.lastIndexOf(":")+1);
            //System.out.println("child suiteName: " + suiteName);

            p = locateSuiteProperties(suiteName, subProps, false, false);
            setSuiteProperties(p, suiteName, subProps, false, false);

            // Now locate the suite runall file containing the tests
            String runfile = "suites" + '/' + suiteName + ".runall";

            InputStream is =  org.apache.derbyTesting.functionTests.harness.RunTest.loadTestResource(runfile);
            if (is == null)
            {
                // Look in userdir
                is = org.apache.derbyTesting.functionTests.harness.RunTest.loadTestResource(userdir + '/' + suiteName + ".runall");
            }
            if (is == null)
            {
                System.out.println("Suite runall file not found for " + suiteName);
                continue;
            }

            // Create a BufferedReader to read the list of tests to run
            runlistFile = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            if (runlistFile == null)
            {
                System.out.println("The suite runall file could not be read.");
            }
            else
            {
                String startTime = CurrentTime.getTime();
                pwOut.println("**** Start SubSuite: " + fullsuiteName +
                    " jdk" + javaVersion +
                    " " + startTime + " ****");
                if ( (framework != null) && (framework.length()>0) )
                {
                    pwOut.println("Framework: " + framework);
                }
                else
                    pwOut.println("Framework: No special framework.");

                // Create the file to list the suites that get skipped
	            File f = new File(outDir, topSuiteName);
	            File skipFile = new File(f, topSuiteName+".skip");

		    //we catch an IOException here to work around a jvm bug on the Psion.
		    PrintStream ps = null;
		    try { ps = new PrintStream
		            ( new FileOutputStream(skipFile.getCanonicalPath(),true) ); }
		    catch (IOException e) {
			FileWriter fw = new FileWriter(skipFile);
			fw.close();
			ps = new PrintStream
			    ( new FileOutputStream(skipFile.getCanonicalPath(),true) );
		    }

                // Due to autoloading of JDBC drivers introduced in JDBC4
                // (see DERBY-930) the embedded driver and Derby engine
                // might already have been loaded.  To ensure that the
                // embedded driver and engine used by the tests run in
                // this suite are configured to use the correct
                // property values we try to unload the embedded driver
                if (useprocess == false) {
                    unloadEmbeddedDriver();
                }

                System.out.println("Now run the suite's tests");
                //System.out.println("shutdownurl: " + shutdownurl);

                if (skip) // Skip a suite under certain environments
				{
				    addToSkipFile(topSuiteName+":"+fullsuiteName, ps);
					if(driverNotFound)
                    	pwOut.println("Cannot run the suite, framework driver not found");
					else if(needSync)
                    	pwOut.println("Cannot run the suite, sync product not found");
					else if(needJdk12ext)
                    	pwOut.println("Cannot run the suite, requires jdk12 or higher with extensions");
					else if(needJdk12)
                    	pwOut.println("Cannot run the suite, requires jdk12 or higher, have jdk" + javaVersion);
					else if(excludedFromJCC)
                    	pwOut.println(clientExclusionMessage);
					else if((needIBMjvm == null || needIBMjvm.booleanValue() == false))
                    	pwOut.println("Cannot run the suite, requires IBM jvm, jvm vendor is " + System.getProperty("java.vendor"));
					else
                    	pwOut.println("Cannot run the suite, have jdk" + javaVersion);
				 }
                else
                {
                    System.out.println("Run the tests...");
                    // Unjar any jarfile define for an upgrade suite
                    //System.out.println("jarfile: " + jarfile);
                    if (jarfile != null)
                    {
                        //System.out.println("unjar jar file...");
                        UnJar uj = new UnJar();
                        uj.unjar(jarfile, outDir.getCanonicalPath(), true);
                        if ( (upgradetest.equals("true")) && (suiteName.startsWith("convert")) )
                        {
                            // need to rename the directory
                            // such as kimono -- rename to convertKimono
			    String tmpname = jarfile.substring(0, jarfile.indexOf("JAR"));
                            File tmp = new File(outDir, tmpname);
			    File convert = new File(outDir, usesystem);
                            boolean renamed = tmp.renameTo(convert);
                            //System.out.println("renamed: " + renamed);
                        }
                    }

                    // Run the tests for this suite
                    runTests(p, fullsuiteName);
                }

                String endTime = CurrentTime.getTime();
                pwOut.println("**** End SubSuite: " + fullsuiteName +
                    " jdk" + javaVersion +
                    " " + endTime + " ****");
                //System.out.println("--------------------------------------");
                ps.close();
            }
        }
    }


    private static void runTests(Properties suiteProps, String suite)
        throws IOException, Exception
    {
	    // save a copy of the system properties at this point; when runing with
	    // java threads we need to reset the system properties to this list;
	    // otherwise we start to accumulate extraneous properties from
	    // individual tests (does not happen with exec (useprocess==true)
	    // because each test case has its own fresh VM
	    ManageSysProps.saveSysProps();

        // Build command string for RunTest()
        StringBuffer sb = new StringBuffer();
	    jvm = jvm.getJvm(jvmName);
	    Vector<String> jvmProps = new Vector<String>();
	    if ((javaCmd.length()>0) )
	    {
	        jvm.setJavaCmd(javaCmd);
	        jvmProps.addElement("javaCmd=" + javaCmd);
	    }
        if ( (testJavaFlags != null) && (testJavaFlags.length()>0) )
            jvmProps.addElement("testJavaFlags=" + testJavaFlags);
	    if (classpath != null)
	        jvmProps.addElement("classpath=" + classpath);
	    if (classpathServer != null)
	        jvmProps.addElement("classpathServer=" + classpathServer);
	    if (jversion != null)
	        jvmProps.addElement("jvm=" + jversion);
        if (framework != null)
            jvmProps.addElement("framework=" + framework);
        if (usesystem != null)
            jvmProps.addElement("usesystem=" + usesystem);
        if (shutdownurl != null)
            jvmProps.addElement("shutdownurl=" + shutdownurl);
        if (upgradetest != null)
            jvmProps.addElement("upgradetest=" + upgradetest);       
        if (outcopy != null)
            jvmProps.addElement("outcopy=" + outcopy);
        if (useoutput != null)
            jvmProps.addElement("useoutput=" + useoutput);
        if (verbose == true)
            jvmProps.addElement("verbose=true");
        if ( (reportstderr != null) && (reportstderr.length()>0) )
            jvmProps.addElement("reportstderr=" + reportstderr);

        if ( (jvmflags != null) && (jvmflags.length()>0) )
        {
            // We want to pass this down to RunTest so it will
            // run an individual test with jvmflags like -nojit
            jvmProps.addElement("jvmflags=" + jvmflags);
        }

        if ( (timeout != null) && (timeout.length()>0) )
        {
            if (useprocess)
			{
                jvmProps.addElement("timeout=" + timeout);            
            }
            else
            {
                org.apache.derbyTesting.functionTests.harness.RunTest.timeoutStr = timeout;
            }
        }
		if (Boolean.getBoolean("listOnly"))
			jvmProps.addElement("listOnly=true");

        if (encryption != null)
            jvmProps.addElement("encryption=" + encryption);
        if (testEncryptionProvider != null)
            jvmProps.addElement("testEncryptionProvider=" + testEncryptionProvider);
        if (testEncryptionAlgorithm != null)
            jvmProps.addElement("testEncryptionAlgorithm=" + testEncryptionAlgorithm);
        if (jdk12test != null)
            jvmProps.addElement("jdk12test=" + jdk12test);
        if (jdk12exttest != null)
            jvmProps.addElement("jdk12exttest=" + jdk12exttest);
        if (keepfiles != null)
            jvmProps.addElement("keepfiles=" + keepfiles);
        if ( (outputdir != null) && (outputdir.length()>0) )
        {
            jvmProps.addElement("outputdir=" + outputdir);
        }
        if ( (topsuitedir != null) && (topsuitedir.length()>0) )
            jvmProps.addElement("topsuitedir=" + topsuitedir);
        else
            jvmProps.addElement("topsuitedir=" + outputdir);
        if (topreportdir != null)
            jvmProps.addElement("topreportdir=" + topreportdir);
        else
            jvmProps.addElement("topreprtdir=" + topsuitedir);
        if ( (runDir != null) && (runDir.exists()) )
            jvmProps.addElement("rundir=" + runDir.getCanonicalPath());
        if ( (bootcp != null) && (bootcp.length()>0) )
            jvmProps.addElement("bootcp=" + bootcp);
        if ( (serverJvm != null) && (serverJvm.length()>0) )
            jvmProps.addElement("serverJvm=" + serverJvm);
        if ( (serverJvmName != null) && (serverJvmName.length()>0) )
            jvmProps.addElement("serverJvmName=" + serverJvmName);
        if (testEncoding != null)
            jvmProps.addElement("derbyTesting.encoding=" + testEncoding);
        if (upgradejarpath != null)
            jvmProps.addElement("derbyTesting.jar.path=" + upgradejarpath);
        if ( (hostName != null) && (hostName.length()>0) )
        	jvmProps.addElement("hostName=" + hostName);
        if ( useprocess == false )
            jvmProps.addElement("useprocess=false");
        if ( skipsed.equals("true") )
            jvmProps.addElement("skipsed=true");
        if ( systemdiff != null )
            jvmProps.addElement("systemdiff=" + systemdiff);
        if ( ijdefaultResourcePackage != null )
            jvmProps.addElement("ij.defaultResourcePackage=" + ijdefaultResourcePackage);
        if ( mtestdir != null )
            jvmProps.addElement("mtestdir=" + mtestdir);
        if (topSpecialProps.length()>0)
        {
            jvmProps.addElement("testSpecialProps=" + topSpecialProps +
            ((otherSpecialProps.length()>0)?
             ("^" + otherSpecialProps)
             :"")
            );
        }
        else if (otherSpecialProps.length()>0)
            jvmProps.addElement("testSpecialProps=" + otherSpecialProps);
            
        if (derbyTestingXaSingle != null)
            jvmProps.addElement ("derbyTesting.xa.single=" + derbyTestingXaSingle);
        
        // Ensure any properties that define the default connection
        // for the tests to use a DataSource are passed from the
        // command line onto the RunTest invoked. These properties are
        //
        // ij.dataSource=<classname of datasource>
        //
        // any number of
        // ij.dataSource.<datasource property>=<value>

        Properties sysProps = System.getProperties();
        for (Enumeration e = sysProps.keys(); e.hasMoreElements(); )
        {
        	String key = (String) e.nextElement();
        	if (key.startsWith("ij.dataSource"))
        		jvmProps.addElement(key + "=" +  sysProps.getProperty(key)); 		
        }

        jvmProps.addElement("suitename=" + suite);

        if ( (topSuiteName != null) && (topSuiteName.length()>0) )
            jvmProps.addElement("topsuitename=" + topSuiteName);

        if (classpath != null)
            jvm.setClasspath(classpath);

        jvm.setD(jvmProps);
        Vector<String> v = jvm.getCommandLine();
        v.addElement("org.apache.derbyTesting.functionTests.harness.RunTest");

        String str = "";
	    String lastTest = null;
	    String skipTo = System.getProperties().getProperty("skipToFile");
	    String stopAfter = System.getProperties().getProperty("stopAfterFile");
        // Read the individual tests
        // Example: "lang/avg.sql" or "conn/resultset.java"
        while ( (str = runlistFile.readLine()) != null )
        {
	        // skip tests if specified
	        if (skipTo != null && !str.equals(skipTo)) 
	            continue;
	        else 
		        skipTo = null;
	        if (stopAfter != null && lastTest != null && lastTest.equals(stopAfter)) break;
            // Create the command for RunTest
            // Create a string array from the vector
            String testCmd[] = new String[v.size() + 1];
            StringBuffer verboseSb = new StringBuffer();
            int i = 0;
            for (i = 0; i < v.size(); i++)
            {
                testCmd[i] = (String)v.elementAt(i);
                verboseSb.append(testCmd[i] + " ");
            }
            testCmd[i++] = str;
            verboseSb.append(str + " ");
            //if (verbose) 
                //System.out.println("Execute command: " + verboseSb.toString());

	        String uc = System.getProperties().getProperty("useCommonDB");
		    if (uc == null) uc = "false";
            if ( useprocess == true && uc.equals("true")==false)
            {
                System.out.println("Execute command: " + verboseSb.toString());

                // Now execute the command to run the test
        		Process pr = null;
        		try
        		{
                    pr = Runtime.getRuntime().exec(testCmd);

                    // We need the process inputstream to capture into the output file
                    BackgroundStreamDrainer stdout =
                        new BackgroundStreamDrainer(pr.getInputStream(), null);
                    BackgroundStreamDrainer stderr =
                        new BackgroundStreamDrainer(pr.getErrorStream(), null);

                    pr.waitFor();

                    String result = HandleResult.handleResult(pr.exitValue(),
                        stdout.getData(), stderr.getData(), pwOut, testEncoding);
                    pr.destroy();
                }
                catch(Throwable t)
                {
                    System.out.println("Process exception: " + t.getMessage());
                    if (pr != null)
                    {
                        pr.destroy();
                        pr = null;
                    }
                }
            }
            else
            {
                // if useprocess=false, we cannot pass properties on a commandline,
                // instead we pass absolutely necessary properties directly to RunTest.
                // At the very minimum, we need to know:
                // 0. the test 
                // 1. resourcepackage - the base for loading functionTests Resources
                // 2. whether or not to use a specific system & the usesystem flag (like for nist)
                // 3. useprocess flag
                // 4. shutdown url 
                // 5. name of the suite
                // 6. the framework, or subsuites might default back to embedded
                // if a test needs a jvm process started with more/other properties than these, 
                // it will not run well with useprocess=false or not in the same way as with
                // useprocess=true
                String[] args = new String[7];
                args[0] = str; // the test name
                if ( ijdefaultResourcePackage != null )
                    args[1] = ijdefaultResourcePackage;
                else
                    args[1] = "/org/apache/derbyTesting/functionTests/";
                if ( usesystem != null )
                    args[2] = usesystem;
                else
                    args[2] = "";
                args[3] = "noprocess";
                if ( shutdownurl != null)
                    args[4] = shutdownurl;
                else
                    args[4] = "";
                args[5] = suite;
                args[6] = framework;
                org.apache.derbyTesting.functionTests.harness.RunTest.main(args);
                // Write any diff to the suite's output
                String tmp = str.substring(str.indexOf("/") + 1, str.lastIndexOf("."));
                String diffname = tmp + "." + "diff";
                File diffFile = new File(outDir, diffname);
                if ( (diffFile != null) && (diffFile.exists()) )
                {
                    BufferedReader inFile =
                        new BufferedReader(new FileReader(diffFile));
                    String diffLine = "";
                    while ( (diffLine = inFile.readLine()) != null )
                    {
                        pwOut.println(diffLine);
                    }
                }

            }
	    // reset the system properties to prevent confusion
	    // when running with java threads
	    ManageSysProps.resetSysProps();
	    lastTest = str;
        }

        // If useprocess is false, and this is a networkserver test,
        // we can speed up the test run by not starting and stopping networkserver
        // for every test (and waiting for it to be up), as we're using the same 
        // directory for all test files (instead of creating each test's files in a new dir). 
        // NetworkServer will get started through RunTest if it's not running, but
        // at the end of a suite run, we need to make sure we shutdown network server
        if ((!useprocess) && ((framework !=null) && (framework.startsWith("DerbyNet"))))
        {
            try 
            { 
                String stopCmd = javaCmd + 
                   " org.apache.derby.drda.NetworkServerControl shutdown";
                Process prstop = Runtime.getRuntime().exec(stopCmd);
            } catch (Exception e) {} // ignore
        }
    }

    /**
    * Locate the suite's properties file
    */
    public static Properties locateSuiteProperties(String suiteName, 
        Properties parentProps, boolean isParent, boolean isTop)
        throws ClassNotFoundException, IOException, Exception
    {
        // Check for suite properties
        //System.out.println("Checking for suite properties");
        String suitePropsName = "suites" + '/' + suiteName + ".properties";

        InputStream is = org.apache.derbyTesting.functionTests.harness.RunTest.loadTestResource(suitePropsName);
        if (is == null)
        {
            // Look in userdir
            suitePropsName = userdir + '/' + suiteName + ".properties";
            is = org.apache.derbyTesting.functionTests.harness.RunTest.loadTestResource(suitePropsName);
        }
        Properties p = new Properties();
        // Reset these properties
        if (isParent)
        {
            usesystem = null;
            upgradetest = null;
            jarfile = null;
            outcopy = null;
            useoutput = null;
            mtestdir = null;
            skipsed = "false";
            //outputdir = outDir.getCanonicalPath();
        }
        if (is != null)
        {
            p.load(is);
            is = null;
        }
        else
        {
            // Reset framework to the parent suite's framework, if any
            // because framework may have been set by previous suite
            testEncoding = parentProps.getProperty("derbyTesting.encoding");
            upgradejarpath = parentProps.getProperty("derbyTesting.jar.path");
            framework = parentProps.getProperty("framework");
            serverJvm = parentProps.getProperty("serverJvm");
            serverJvmName = parentProps.getProperty("serverJvmName");
            // Do the same for ij.defaultResourcePackage
            ijdefaultResourcePackage =
                parentProps.getProperty("ij.defaultResourcePackage");
            // And do the same for encryption
            encryption = parentProps.getProperty("encryption");
            testEncryptionProvider = parentProps.getProperty("testEncryptionProvider");
            testEncryptionAlgorithm = parentProps.getProperty("testEncryptionAlgorithm");
            // And do the same for jdk12test
            jdk12test = parentProps.getProperty("jdk12test");
            jdk12exttest = parentProps.getProperty("jdk12exttest");
	        runwithj9 = parentProps.getProperty("runwithj9");
            runwithibmjvm = parentProps.getProperty("runwithibmjvm");
            String testJVM = jvmName;
            if (jvmName.startsWith("j9") && (!jvmName.startsWith("j9dee")))
            	testJVM = (jvmName.startsWith("j9_foundation") ? "foundation" : "j9");            
            runwithjvm = parentProps.getProperty("runwith" + testJVM);
            excludeJCC = parentProps.getProperty("excludeJCC");
        }                
        return p;
    }


    /**
    * Properties which may be defined for all suites
    * at the top level suite (such as "nightly")
    */
    private static void setTopSuiteProperties()
        throws ClassNotFoundException, IOException
    {
		framework = suiteProperties.getProperty("framework");
		
		jversion = suiteProperties.getProperty("jversion");
		//System.out.println("RunList top jversion= " + jversion);
		
		jvmName = suiteProperties.getProperty("jvm");
		String j9config = System.getProperty("com.ibm.oti.configuration");	
		if (j9config != null)
			if (j9config.equals("foun10")) 
				jvmName="j9_foundation";
			else if (j9config.equals("foun11"))
				jvmName="j9_foundation11";
			else if (j9config.equals("max"))
				jvmName="j9_13";
			else if (j9config.equals("dee"))
				jvmName="j9dee15";

		if (jversion == null)
		    javaVersion = System.getProperty("java.version");
		else
		    javaVersion = jversion;
		    
		//System.out.println("RunList setTopSuiteProperties javaVersion: " + javaVersion);

		javaCmd = suiteProperties.getProperty("javaCmd");
		if (javaCmd == null)
		    javaCmd = "java";
		else if (javaCmd.equals("jview"))
		    jvmName = "jview";

		// if j9, we need to check further
		String javavmVersion;
		if (System.getProperty("java.vm.name").equals("J9"))
			javavmVersion = (System.getProperty("java.vm.version"));
		else
			javavmVersion = javaVersion;


        JavaVersionHolder jvh = new JavaVersionHolder(javavmVersion);
        majorVersion = jvh.getMajorVersion();
        minorVersion = jvh.getMinorVersion();
        iminor = jvh.getMinorNumber();
        imajor = jvh.getMajorNumber();

		if ( (jvmName == null) || (jvmName.equals("jview")) )
		{
		    if ( (iminor < 2) && (imajor < 2) )
		        jvmName = "currentjvm";
		    else
		        jvmName = "jdk" + majorVersion + minorVersion;
		}
	
		if (jvmName.equals("j9_13"))
		{ 
			javaVersion = javaVersion + " - " + majorVersion + "." + minorVersion;
			System.out.println("javaVersion now: " + javaVersion);
			// up to j9 2.1 (jdk 1.3.1. subset) the results are the same for all versions, or
			// we don't care about it anymore. So switch back to 1.3 (java.version values).
			if ((imajor <= 2) && (iminor < 2))
			{
				majorVersion = "1";
				minorVersion = "3";
				imajor = 1;
				iminor = 3;
			}
			else 
				jvmName = "j9_" + majorVersion + minorVersion;
		}

		jvmflags = suiteProperties.getProperty("jvmflags");
		testJavaFlags = suiteProperties.getProperty("testJavaFlags");
		classpath = suiteProperties.getProperty("classpath");
		classpathServer = suiteProperties.getProperty("classpathServer");
		usesystem = suiteProperties.getProperty("usesystem");
		upgradetest = suiteProperties.getProperty("upgradetest");
        outcopy = suiteProperties.getProperty("outcopy");
		useoutput = suiteProperties.getProperty("useoutput");
		encryption = suiteProperties.getProperty("encryption");
		testEncryptionProvider = suiteProperties.getProperty("testEncryptionProvider");
		testEncryptionAlgorithm = suiteProperties.getProperty("testEncryptionAlgorithm");
		jdk12test = suiteProperties.getProperty("jdk12test");
		jdk12exttest = suiteProperties.getProperty("jdk12exttest");
		runwithibmjvm = suiteProperties.getProperty("runwithibmjvm");
		runwithj9 = suiteProperties.getProperty("runwithj9");
                derbyTestingXaSingle = suiteProperties.getProperty("derbyTesting.xa.single");
        String testJVM = jvmName;
        if (jvmName.startsWith("j9") && (!jvmName.startsWith("j9dee")))
        	testJVM = (jvmName.startsWith("j9_foundation") ? "foundation" : "j9");
        runwithjvm = suiteProperties.getProperty("runwith" + testJVM);
		excludeJCC = suiteProperties.getProperty("excludeJCC");
		keepfiles = suiteProperties.getProperty("keepfiles");
		systemdiff = suiteProperties.getProperty("systemdiff");
		outputdir = suiteProperties.getProperty("outputdir");
		if (outputdir == null)
		    outputdir = userdir;
		topsuitedir = suiteProperties.getProperty("topsuitedir");
		if (topsuitedir == null)
		    topsuitedir = outputdir;
		bootcp = suiteProperties.getProperty("bootcp");
		serverJvm = suiteProperties.getProperty("serverJvm");
		serverJvmName = suiteProperties.getProperty("serverJvmName");
		hostName = suiteProperties.getProperty("hostName");
		testEncoding = suiteProperties.getProperty("derbyTesting.encoding");
		upgradejarpath = suiteProperties.getProperty("derbyTesting.jar.path");
		canondir = suiteProperties.getProperty("canondir");
		mtestdir = suiteProperties.getProperty("mtestdir");
		String usepr = suiteProperties.getProperty("useprocess");
		if ( (usepr != null) && (usepr.equals("false")) )
		    useprocess = false;
		skipsed = suiteProperties.getProperty("skipsed");
		String dbug = suiteProperties.getProperty("verbose");
		if ( (dbug != null) && (dbug.equals("true")) )
		    verbose = true;
		reportstderr = suiteProperties.getProperty("reportstderr");
		timeout = suiteProperties.getProperty("timeout");
		shutdownurl = suiteProperties.getProperty("shutdownurl");
		topSuiteName = suiteProperties.getProperty("suitename");
		ijdefaultResourcePackage =
		    suiteProperties.getProperty("ij.defaultResourcePackage");
        // The top level suiteProperties may have special
        // properties which need to be added to testSpecialProps
        if ( (specialProperties != null) && (!specialProperties.isEmpty()) )
        {
            //System.out.println("Top suite has special props");
            setSpecialProps(specialProperties, true);
        }
    }

    /**
    * Properties for nested suites
    */
    private static void setSuiteProperties(Properties p, String suiteName,
        Properties parentProperties, boolean isParent, boolean isTop)
        throws ClassNotFoundException, IOException
    {
        // Some properties may have been set by the top suite
        // jvm, jvmflags, classpath, systemdiff, verbose, etc.
        // In that case, these will be preserved for the rest
        if (jversion != null)
            p.put("jvm", jversion);
    	if ( jvmName == null )
    		jvmName = "currentjvm";
    	else
    		p.put("jvm", jvmName);

        if ( javaCmd == null )
            javaCmd = "java";
        else
            p.put("javaCmd", javaCmd);

        // all jvmflags should get appended, with command line overwrite top suite 
        // properties overwrite lower level suite properties
        // but we're letting the jvm handle that by putting the cmdline last.
        // note that at this point, the parentproperties already should have appended the
        // jvmflags from the command line and the top suite properties file
        // only need to add the lower suite properties in the mix
        String totaljvmflags = jvmflags;
        String subjvmflags = p.getProperty("jvmflags");
        String parentjvmflags = parentProperties.getProperty("jvmflags");
        
        if ((subjvmflags != null) && (parentjvmflags != null) && (!subjvmflags.equals(parentjvmflags)))
        {
            //DERBY-4680 Make sure ^ does not get prepended to jvmflags
            if (subjvmflags != null &&  subjvmflags.length() > 0)
                totaljvmflags = subjvmflags + "^" + totaljvmflags;
        }
        if (totaljvmflags != null)
        {
            jvmflags= totaljvmflags;
        }

    	if ( classpath != null )
    		p.put("classpath", classpath);
    	if ( classpathServer != null )
    		p.put("classpathServer", classpathServer);
        if ( systemdiff != null )
            p.put("systemdiff", systemdiff);
        if ( verbose == true )
            p.put("verbose", "true");
        if ( bootcp != null )
            p.put("bootcp", "bootcp");
        if ( canondir != null )
            p.put("canondir", canondir);

		if ( (outputdir == null) || (outputdir.length() == 0) )
		{
		    outputdir = p.getProperty("outputdir");
		    if (outputdir == null)
		        outputdir = userdir;
		}

	    // framework may be set at the top, or just
	    // set for individual suites
	    if ( parentProperties.getProperty("framework") != null )
		    p.put("framework", framework);
		else
            framework = p.getProperty("framework");

		// same for serverJvm and serverJvmName
        if ( parentProperties.getProperty("serverJvm") != null )
            p.put("serverJvm", serverJvm);
		else
            serverJvm = p.getProperty("serverJvm");
        if ( parentProperties.getProperty("serverJvmName") != null )
            p.put("serverJvmName", serverJvmName);
		else
            serverJvmName = p.getProperty("serverJvmName");
        
        // derbyTesting.encoding may be set at the top, or just
        // set for individual suites
        if(parentProperties.getProperty("derbyTesting.encoding") != null)
		    p.put("derbyTesting.encoding", testEncoding);
		else
            testEncoding = p.getProperty("derbyTesting.encoding");

        if(parentProperties.getProperty("derbyTesting.jar.path") != null)
		    p.put("derbyTesting.jar.path", upgradejarpath);
		else
            upgradejarpath = p.getProperty("derbyTesting.jar.path");

        if ( hostName != null )
            p.put("hostName", hostName);
        else
        	p.put("hostName","localhost");
        // Encryption may be set at the top or just for a subsuite
	    if ( parentProperties.getProperty("encryption") != null )
		    p.put("encryption", encryption);
		else
            encryption = p.getProperty("encryption");

	// Encryption provider may be set at the top or just for a subsuite
	    if ( parentProperties.getProperty("testEncryptionProvider") != null )
		    p.put("testEncryptionProvider", testEncryptionProvider);
		else
            testEncryptionProvider = p.getProperty("testEncryptionProvider");

	// Encryption algorithm may be set at the top or just for a subsuite
	    if ( parentProperties.getProperty("testEncryptionAlgorithm") != null )
		    p.put("testEncryptionAlgorithm", testEncryptionAlgorithm);
		else
            testEncryptionAlgorithm = p.getProperty("testEncryptionAlgorithm");

        // jdk12test may be set at the top or just for a subsuite
	    if ( parentProperties.getProperty("jdk12test") != null )
		    p.put("jdk12test", jdk12test);
		else
            jdk12test = p.getProperty("jdk12test");

        // jdk12exttest may be set at the top or just for a subsuite
	    if ( parentProperties.getProperty("jdk12exttest") != null )
		    p.put("jdk12exttest", jdk12exttest);
		else
            jdk12exttest = p.getProperty("jdk12exttest");

        // runwithibmjvm may be set at the top or just for a subsuite
	    if ( parentProperties.getProperty("runwithibmjvm") != null )
		    p.put("runwithibmjvm", runwithibmjvm);
		else
            runwithibmjvm = p.getProperty("runwithibmjvm");

        // runwithjvm may be set at the top or just for a subsuite
	    String testJVM = jvmName;
        if (jvmName.startsWith("j9") && (!jvmName.startsWith("j9dee")))
        	testJVM = (jvmName.startsWith("j9_foundation") ? "foundation" : "j9");
	    if ( parentProperties.getProperty("runwith" + testJVM) != null )
		    p.put("runwith" + testJVM, runwithjvm);
		else
            runwithjvm = p.getProperty("runwith" + testJVM);

        // runwithj9 may be set at the top or just for a subsuite
	    if ( parentProperties.getProperty("runwithj9") != null )
		    p.put("runwithj9", runwithj9);
		else
            runwithj9 = p.getProperty("runwithj9");

        // excludeJCC may be set at the top or just for a subsuite
	    if ( parentProperties.getProperty("excludeJCC") != null )
		    p.put("excludeJCC", excludeJCC);
		else
            excludeJCC = p.getProperty("excludeJCC");

        // useprocess may be set at the top or just for a subsuite
        String upr = parentProperties.getProperty("useprocess");
	    if ( upr != null )
		    p.put("useprocess", upr);
		else
		{
            upr = p.getProperty("useprocess");
            if ( upr == null)
                useprocess = true;
            else if (upr.equals("false"))
                useprocess = false;
            else
                useprocess = true;
        }
		// properties specific to a single suite
		usesystem = p.getProperty("usesystem");
		shutdownurl = p.getProperty("shutdownurl");
        upgradetest = p.getProperty("upgradetest");
        jarfile = p.getProperty("jarfile");
        skipsed = p.getProperty("skipsed");
        if (skipsed == null)
            skipsed = "false";
		if ( "true".equals(keepfiles) )
		    p.put("keepfiles", keepfiles);

        // testJavaFlags should get appended

        String testflags = p.getProperty("testJavaFlags");
        if ( parentProperties.getProperty("testJavaFlags") != null )
        {
            if ( (testflags != null) && (!testflags.equals(testJavaFlags)) )
            {
                testJavaFlags = testJavaFlags + "^" + testflags;
            }
            p.put("testJavaFlags", testJavaFlags);
        }
        else
            testJavaFlags = p.getProperty("testJavaFlags");

		// The following could change between suites or
		// may be set for the whole set of suites

        if ( parentProperties.getProperty("reportstderr") != null )
            p.put("reportstderr", reportstderr);
        else
            reportstderr = p.getProperty("reportstderr");

        if ( parentProperties.getProperty("timeout") != null )
            p.put("timeout", timeout);
        else
            timeout = p.getProperty("timeout");

        // outcopy is very specific to a single suite
        outcopy = p.getProperty("outcopy");

		// useoutput is very specific to a single suite
		useoutput = p.getProperty("useoutput");


        // mtestdir is very specific to a multi suite
        mtestdir = p.getProperty("mtestdir");

        // ijdefaultResourcePackage is specific for a suite
        ijdefaultResourcePackage = p.getProperty("ij.defaultResourcePackage");

		if ( topSuiteName == null )
		    topSuiteName = p.getProperty("suitename");
		else
		    p.put("suitename", topSuiteName);

		skip = shouldSkipTest();
		    
        // Set the suite subdir under top outputdir
        setSuiteDir(suiteName, isParent, isTop);

        // This individual suite may also have special flags
        // Reset otherSpecialProps in case another suite had any set
        otherSpecialProps = "";
        Properties specialProps = SpecialFlags.getSpecialProperties(p);
        if ( (specialProps != null) && (!specialProps.isEmpty()) )
            // Add any special properties to suiteJavaFlags string
            setSpecialProps(specialProps, false);
    }

	/**
		Determine if a test should be skipped or not.
		These are ad-hoc rules, see comments within for details.
		Examples of what is checked: JVM version, framework,
		encryption, jdk12test, 
		Sets some global variables so that skip reporting is clearer.

		@return true if test should not be run.
    */
    private static boolean shouldSkipTest()
    {
	boolean result = false;

	// figure out if suite should be skipped ... adhoc rules
	boolean isJdk12 = false; // really now 'isJdk12orHigher'
	boolean isJdk118 = false;
	boolean isJdk117 = false;
	boolean isEncryption = false;
	boolean isJdk12Test = false;
	boolean isJdk12ExtTest = false;
	boolean isSyncTest = false;
	boolean isSyncProduct = false;
	boolean isExcludeJCC = false;
	// runwithibmjvm is really tri-state. null = run-anywhere,
	// true = only ibm jvms, false = only non-IBM jvms.

	// reset skip reason parameters
	driverNotFound = false;
	needSync = false;
	needJdk12 = false;
	needJdk12ext = false;
	excludedFromJCC = false;
	needIBMjvm = null;


	// Determine if this is jdk12 or higher (with or without extensions)
    if (iminor >= 2) isJdk12 = true;
	if ( System.getProperty("java.version").startsWith("1.1.8") ) isJdk118 = true;
    if ( System.getProperty("java.version").startsWith("1.1.7") ) isJdk117 = true;
    
	// if a test needs an ibm jvm, skip if runwithibmjvm is true.
	// if a test needs to not run in an ibm jvm, skip if runwithibmjvm is false.
	// if null, continue in all cases.
	if (runwithibmjvm != null) 
	{ 
	    if (runwithibmjvm.equals("")) { needIBMjvm = null; }
	    else { needIBMjvm = Boolean.valueOf(runwithibmjvm); }
	}
	if (runwithibmjvm == null) { needIBMjvm = null; }
	if (needIBMjvm != null)
	{
	    boolean needsibm = needIBMjvm.booleanValue();
	    boolean ibmjvm = false;
	    String vendor = System.getProperty("java.vendor");
	    if (vendor.startsWith("IBM")) { ibmjvm = true; }
	    if (!needsibm && ibmjvm) { return true; }
	    if (needsibm && !ibmjvm) { return true; }
	}

	if (runwithjvm != null && runwithjvm.equals("false"))
	{
	    return true;
	}

        if ( (framework != null) && (framework.length()>0) )
	{
            if (framework.equals("DerbyNet"))
	    {
		// skip if the derbynet.jar is not in the Classpath
		try {
			Class.forName("org.apache.derby.drda.NetworkServerControl");
		} catch (ClassNotFoundException cnfe) {
			driverNotFound = true;
			result = true;
		}

		// skip if the IBM Universal JDBC Driver is not in the Classpath
		// note that that driver loads some javax.naming.* classes which may not
		// be present at runtime, and thus we need to catch a possible error too 
		try {
			Class.forName("com.ibm.db2.jcc.DB2Driver");
		} catch (ClassNotFoundException cnfe) {
			driverNotFound = true;
			result = true;
		} catch (NoClassDefFoundError err) {
			driverNotFound = true;
			result = true;
		}
	    }
	}

	if (result) return true; // stop looking once know should skip

        if ( (encryption != null) && (encryption.length()>0) )
            if ("true".equalsIgnoreCase(encryption)) isEncryption = true;
        if ( (jdk12test != null) && (jdk12test.length()>0) )
            if ("true".equalsIgnoreCase(jdk12test)) isJdk12Test = true;		
        if ( (jdk12exttest != null) && (jdk12exttest.length()>0) )
            if ("true".equalsIgnoreCase(jdk12exttest)) isJdk12ExtTest = true;
        
        // Skip any suite if jvm is not jdk12 or higher for encryption, jdk12test or jdk12exttest
        if (!isJdk12)
        {
            if ( (isEncryption) || (isJdk12Test) || (isJdk12ExtTest) )
            {
                needJdk12 = true;
                result = true; // Can't run in this combination
            }
	    if (result) return true; // stop looking once know should skip
 	}		

    // Also require jdk12 extensions for encryption and jdk12exttest
	if ( (isEncryption) || (isJdk12ExtTest) )
	{
	    needJdk12ext = true;
            // Check for extensions
            try
            {
                Class jtaClass = Class.forName("javax.transaction.xa.Xid");
                Class jdbcClass = Class.forName("javax.sql.RowSet");
            } 
            catch (ClassNotFoundException cnfe)
            {
                // at least one of the extension classes was not found
                result = true; // skip this test
            }			
	    if (result) return true; // stop looking once know should skip
        }

        if (isEncryption)  // make sure encryption classes are available
        {
            needEncryption = true;
            try 
            {
                Class jceClass = Class.forName("javax.crypto.Cipher");
            }
            catch (ClassNotFoundException cnfe)
            {
                result = true;
            }
            if (result) return true;
        }

	if (excludeJCC != null)
	{
	    Class<?> c = null;
	    Method m = null;
	    Object o = null;
	    Integer i = null;
	    int jccMajor = 0;
	    int jccMinor = 0;
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
	    } catch (Exception e) {
	        if (verbose) System.out.println("Exception in shouldSkipTest: " + e);
            }

		try {
			checkClientExclusion(excludeJCC, "JCC", jccMajor, jccMinor, javaVersion);
		} catch (Exception e) {
			excludedFromJCC = true;
			clientExclusionMessage = e.getMessage();
			return true;
		}
    }

	return result; // last test result is returned
    }


    public static void setSuiteDir(String suiteName, boolean isParent, boolean isTop)
        throws IOException
    {
        if (isTop) // This is the very top suite for this RunList
        {
            // Here we want to set the topsuitedir
		    if ( (topsuitedir == null) || (topsuitedir.length() == 0) )
		    {
		        topsuitedir = userdir;
		        outputdir = topsuitedir;
		    }
		    else
		        outputdir = topsuitedir;
		    
		    // Create the topsuite directory under the outputdir
		    File topdir = new File(outputdir, topSuiteName);
		    topdir.mkdir();
		    if (!topParentSuite.equals(topSuiteName))
		    {
		        File topparent = new File(topdir, topParentSuite);
                topparent.mkdir();
                outputdir = topparent.getCanonicalPath();
            }
            else
                outputdir = topdir.getCanonicalPath();
            topreportdir = outputdir;
            //System.out.println("RunList topsuitedir: " + outputdir);
            //System.out.println("RunList outputdir: " + outputdir);
            //System.out.println("RunList topreportdir: " + topreportdir);

            // Modify outputdir for special framework
            if ( (framework != null) && (framework.length()>0) )
            {
                File f = new File(outputdir, framework);
                f.mkdir();
                outputdir =  f.getCanonicalPath();
		        fw_set = true; // framework dir set at top level
                //System.out.println("RunList for framework outputdir: " + outputdir);
            }
		    topsuitedir = outputdir; 	
		}
		else if (isParent) // reset outputdir to topsuitedir for a new parent
		{
		    outputdir = topsuitedir;
		    //System.out.println("outputdir reset for parent: " + outputdir);
		    if (!suiteName.equals(topParentSuite))
		    {
                File suitedir = new File(outputdir, suiteName);
                suitedir.mkdir();
                outputdir = suitedir.getCanonicalPath();
            }
            // Modify outputdir for special framework (if not already set)
            if (!fw_set)
            {
                if ( (framework != null) && (framework.length()>0) )
                {
                    File f = new File(outputdir, framework);
                    f.mkdir();
                    outputdir =  f.getCanonicalPath();
                }
            }
        }

		else if ( upgradetest == null ) // this is a child suite of a parent
		{
            File suitedir = new File(outputdir, suiteName);
            suitedir.mkdir();
            outputdir = suitedir.getCanonicalPath();
            //System.out.println("Child outputdir: " + outputdir);
        }
    }

	private static void setSpecialProps(Properties p, boolean isTop)
	{
        // Just build  string for RunTest to parse (^ is the separator)
        // and determine which special flags are for ij or for server
        // These special flags come from specialProperties, not from
        // the usual properties (RunSuite will give these for the top suite)
        String tmp = "";
		for (Enumeration e = p.propertyNames(); e.hasMoreElements(); )
		{
			String key = (String)e.nextElement();
			// Note: RunSuite will already have excluded
			// suites, useoutput, usesystem,keepfiles from these
			tmp += key + "=" + p.getProperty(key) + "^";
		}
		if (tmp.length()>0)
		{
		    if ( isTop == true ) // This is the top level suite
		        topSpecialProps = tmp.substring(0, tmp.lastIndexOf('^'));
		    else // This is a nested suite, do not apply to all the suites
		        otherSpecialProps = tmp.substring(0, tmp.lastIndexOf('^'));
		}
	}
	
	static void addToSkipFile(String suiteName, PrintStream ps) throws IOException
	{
		ps.println(suiteName);
		ps.flush();
    }
	
    /* ****
     * Look at the received exclusion property and use it to
     * figure out if this test/suite should be skipped based
     * on the actual client and JVM versions in question.
     * @param exclusion The harness property indicating the
     *  rules for skipping this test.  For example:
     *  "at-or-before:2.0,when-at-or-after:jdk1.5.1".
     * @param clientName Name of the client being used.
     * @param clientMajor The 'major' part of the client version
     *  that is actually being used for the test.
     * @param clientMinor The 'minor' part of the client version
     *  that is actually being used for the test.
     * @param javaVersion JVM being used, as a string.
     * @return Exception is thrown if this test/suite should
     *  be skipped; else we simply return.
     */
    public static void checkClientExclusion(String exclusion,
        String clientName, int clientMajor, int clientMinor,
        String javaVersion) throws Exception
    {

        if (exclusion == null)
        // we already know the test isn't excluded.
            return;

        // These tell us whether we want to 1) exclude version
        // numbers that are lower than the target version, or
        // 2) exclude version numbers that are higher than the
        // target version.
        int clientComparisonType = 0;
        int jvmComparisonType = 0;

        exclusion = exclusion.toLowerCase();
        String clientVersion = null;

        // Figure out what kind of comparison we need for the client version.
        int comma = exclusion.indexOf(",");
        if (comma != -1)
            clientVersion = exclusion.substring(0, comma);
        else
            clientVersion = exclusion;

        try {
            clientComparisonType = getVersionCompareType(clientVersion);
        } catch (Exception e) {
            System.out.println("exclusion property poorly formatted: " + exclusion);
            return;
        }

        // Figure out what kind of comparison we need for the JVM version.
        boolean jvmDependent;
        if (comma == -1)
            jvmDependent = false;
        else {
            jvmDependent = true;
            // "+6" in next line is length of ",when-", which is the
            // keyword used to begin the jvm exclusion string.
            String jvmVersion = exclusion.substring(comma+6);
            try {
                jvmComparisonType = getVersionCompareType(jvmVersion);
            } catch (Exception e) {
                System.out.println("exclusion property poorly formatted: " + exclusion);
                return;
            }
        }

        // Load the client and JVM target versions.  The "5" in the
        // next line means that we want to parse out 5 numbers from
        // the property: 2 numbers for the client version (ex. "2.0")
        // and 3 numbers for the JVM version (ex. "1.5.1").
        int [] excludeInfo = null;
        try {
            excludeInfo = getVersionArray(exclusion, 5);
        } catch (Exception e) {
            System.out.println("Unexpected text in exclusion property: " + e.getMessage());
            return;
        }

        // Now check to see if this test/suite should be excluded.
        // First check the client version.
        if (versionExcluded(new int [] {clientMajor, clientMinor}, 0,
            excludeInfo, 0, 2, clientComparisonType))
        {

            if (!jvmDependent) {
            // then skip it regardless of JVM.
                throw new Exception("This test/suite is excluded from running with " +
                    clientName + " versions at or " +
                    (clientComparisonType == -1 ? "before " : "after ") +
                    excludeInfo[0] + "." + excludeInfo[1] + ".");
            }

            // Now check the JVM version.
            int [] jvmInfo = null;
            try {
                jvmInfo = getVersionArray(javaVersion, 3);
            } catch (Exception e) {
                System.out.println("Unexpected text in exclusion property: " + e.getMessage());
                return;
            }

            if (versionExcluded(jvmInfo, 0, excludeInfo, 2, 3, jvmComparisonType)) {
                throw new Exception("This test/suite is excluded from running with " +
                    clientName + " versions at or " +
                    (clientComparisonType == -1 ? "before " : "after ") +
                    excludeInfo[0] + "." + excludeInfo[1] + " when JVM versions at or " +
                    (jvmComparisonType == -1 ? "before " : "after ") +
                    excludeInfo[2] + "." + excludeInfo[3] + "." + excludeInfo[4] +
                    " are being used.");
            }
        }

    }

    /* ****
     * Parses a versionString property and returns the specified
     * number of integers as found in that string.  If the number
     * of integers requested is larger than the number of integers
     * found in the version string, -1 will be used as filler.
     *
     * An example versionString might be any of the following:
     * "2.4" or "at-or-after:2.4" or "when:jdk1.3.1" or 
     * "when-at-or-after:jdk1.3.1", etc.  In these examples,
     * the resultant int arrays would be:
     *
     * "2.4"                        ==> [2,4]         // if resultSize == 2.
     * "at-or-after:2.4"            ==> [2.4]         // if resultSize == 2.
     * "when:jdk1.3.1"              ==> [1,3,1]       // if resultSize == 3.
     * "when-at-or-after:jdk1.3.1"  ==> [1,3,1,-1]    // if resultSize == 4.
     *
     * @param versionString The version string to parse.
     * @param resultSize The number of integers to parse out of the
     *   received version string.
     * @return An integer array holding resultSize integers as parsed
     *   from the version string (with -1 as a filler if needed).
     */
    private static int [] getVersionArray(String versionString, int resultSize)
        throws Exception
    {

        if (versionString == null)
        // Use an empty string so that tokenizer will still work;
        // result will be an array of "-1" values.
            versionString = "";

        int [] result = new int[resultSize];

        String tok = null;
        String text = null;
        StringTokenizer st = new StringTokenizer(versionString, ".,_");
        for (int i = 0; i < resultSize; i++) {
    
            if (!st.hasMoreTokens()) {
            // if we're out of integers, use -1 as a filler.
                result[i] = -1;
                continue;
            }

            // Get the token and parse out an integer.
            tok = st.nextToken();
            int pos = 0;
            for (; !Character.isDigit(tok.charAt(pos)); pos++);
            text = tok.substring(0, pos);

            // If we have text, make sure it's a valid keyword
            // and then move past it.
            if ((text.length() > 0) && !isClientExclusionKeyword(text))
                throw new Exception(text);

            // Load the int.
            tok = tok.substring(pos);
            if (tok.length() == 0) {
            // no integer found, so don't count this iteration.
                i--;
                continue;
            }

            result[i] = Integer.parseInt(tok);

        }

        return result;

    }

    /* ****
     * Looks at a version string and searches for an indication
     * of what kind of versions (lower or higher) need to be
     * excluded.  This method just looks for the keywords
     * "at-or-before" and "at-or-after", and then returns
     * a corresponding value.  If neither of those keywords
     * is found, the default is to exclude versions that are
     * lower (i.e. "at-or-before").
     * @param versionString The version string in question,
     *  for example "2.4" or "jdk1.3.1" or "at-or-before:jdk1.3.1".
     * @return -1 if we want to exclude versions that come
     *  before the target, 1 if we want to exclude versions
     *  that come after the target.  Default is -1.
     */
    private static int getVersionCompareType(String versionString)
        throws Exception
    {

        if (versionString == null)
        // just return the default.
            return -1;

        int colon = versionString.indexOf(":");
        if (colon != -1) {
            if (versionString.startsWith("at-or-before"))
                return -1;
            else if (versionString.startsWith("at-or-after"))
                return 1;
            else
                throw new Exception("bad exclusion property format");
        }

        return -1;

    }

    /* ****
     * Takes two versions, each of which is an array of integers,
     * and determines whether or not the first (actual) version
     * should be excluded from running based on the second (target)
     * version and on the received comparisonType.
     * 
     * For example, let vActual be [2,1] and vTarget be [2,4]. Then
     * if comparisonType indicates that versions "at or before" the
     * the target version (2.4) should be excluded, this method
     * would return true (because 2.1 is before 2.4); if comparisonType
     * indicates that versions "at or after" the target type should
     * be excluded, this method would return false (because 2.1 is
     * NOT at or after 2.4).
     *
     * @param vActual The actual version, as an int array.
     * @param vTarget The target version, as an int array.
     * @param offset1 Offset into vActual at which to start the
     *  comparison.
     * @param offset2 Offset into vTarget at which to start the
     *  comparison.
     * @param numParts The maximum number of integer parts to compare.
     * @param comparisonType -1 if we want to exclude versions
     *  at or before the target; 1 if we want to exclude versions
     *  at or after the target.
     * @return True if the actual version should be excluded from
     *  running, false otherwise.
     */
    private static boolean versionExcluded(int [] vActual, int offset1,
        int [] vTarget, int offset2, int numParts, int comparisonType)
    {

        // Figure out how many integer parts we can actually compare.
        // The max is "len", but if len is greater than the length of
        // either of the versions, then we have to compensate for
        // the shortest version.
        int compareLen = (vActual.length >= vTarget.length ? vTarget.length : vActual.length);
        compareLen = (compareLen <= numParts ? compareLen : numParts);

        // Now do the comparison.
        for (int i = 0; i < compareLen; i++) {

            if (comparisonType * vActual[offset1] > comparisonType * vTarget[offset2])
                return true;

            if (comparisonType * vActual[offset1] < comparisonType * vTarget[offset2])
                return false;

            offset1++;
            offset2++;

        }

        // If we get here, the two versions are the same thru
        // compareLen parts.  If that's as far as we're supposed
        // to compare, then we treat them as equal.  Else, we take
        // the version having more parts as the greater of the two.

        if (compareLen == numParts)
        // treat them as equal.
            return true;

        return (comparisonType * vActual.length > comparisonType * vTarget.length);

    }

    /* ****
     * Checks to see if the received string is a recognized
     * keyword for an exclusion property.
     * @param text The string in question.
     * @return True if the received text is a valid keyword
     *  for exclusion properties; false otherwise.
     */
    private static boolean isClientExclusionKeyword(String text) {

        for (int i = 0; i < clientExclusionKeywords.length; i++) {
            if (clientExclusionKeywords[i].equals(text))
                return true;
        }

        return false;

    }


    /**
     * Unloads the embedded JDBC driver and Derby engine in case
     * is has already been loaded. 
     * The purpose for doing this is that using an embedded engine
     * that already is loaded makes it impossible to set new 
     * system properties for each individual suite or test.
     */
    private static void unloadEmbeddedDriver() {
        // Attempt to unload the embedded driver and engine
        // but only if we're not having a J2ME configuration i.e. no DriverManager, so check...
        if (TestUtil.HAVE_DRIVER_CLASS)
        try {
            DriverManager.getConnection("jdbc:derby:;shutdown=true");
        } catch (SQLException se) {
            // Ignore any exception thrown
        }

        // Call the garbage collector as spesified in the Derby doc
        // for how to get rid of the classes that has been loaded
        System.gc();
    }
}

