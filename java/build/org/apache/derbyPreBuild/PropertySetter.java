/*

   Derby - Class org.apache.derbyPreBuild.PropertySetter

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

package org.apache.derbyPreBuild;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;

import java.util.jar.JarFile;
import java.util.jar.Manifest;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.PropertyHelper;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.taskdefs.Property;


/**
 * <p>
 * This tool tries to set the classpath properties if they are not already
 * set:
 * </p>
 *
 * <ul>
 * <li>java14compile.classpath</li>
 * <li>java15compile.classpath</li>
 * <li>java16compile.classpath</li>
 * <li>java17compile.classpath</li>
 * </ul>
 *
 * <p>
 * If the following library properties are set, they may influence how we set the
 * corresponding classpath properties:
 * </p>
 *
 * <ul>
 * <li>j14lib</li>
 * <li>j15lib</li>
 * <li>j16lib</li>
 * <li>j17lib</li>
 * </ul>
 *
 * <p>
 * This tool behaves as follows:
 * </p>
 *
 * <ul>
 * <li>If the classpath properties are set, nothing happens and we simply exit.</li>
 * <li>Otherwise, if a library property is set, we attempt to set the
 * corresponding classpath property to be a list of all the jars in the
 * directory pointed to by the library property.</li>
 * <li>Otherwise we try to set the classpath properties to values
 * specific to the vendor of the running vm.</li>
 * <li>If we don't recognize the vendor of the running vm, print a warning
 * message and then try to set the classpath properties using the JDK with the
 * highest implementation version from any vendor matching the required
 * specification version. If a vendor has chosen to deviate significantly from
 * the file layout of other JDKs, the detection will most likely fail.
 * People using JDKs with a more exotic file layout should specify the library
 * directory explicitly through ant.properties, or resort to setting the compile
 * classpath manually.
 * </li>
 * </ul>
 *
 * <p>
 * This tool has been tested for the setting of jdk1.4 and Java 5 compiler
 * properties in the following environments:
 * </p>
 *
 * <ul>
 * <li>Apple JDKs on Mac OS X</li>
 * <li>Sun and IBM JDKs on Linux</li>
 * <li>Sun and IBM JDKs on Windows/Cygwin</li>
 * <li>Sun JDKs on Solaris</li>
 * </ul>
 */
public class PropertySetter extends Task
{
    /////////////////////////////////////////////////////////////////////////
    //
    //  CONSTANTS
    //
    /////////////////////////////////////////////////////////////////////////

    private static  final   String  J14LIB = "j14lib";
    private static  final   String  J14CLASSPATH = "java14compile.classpath";
    private static  final   String  J15LIB = "j15lib";
    private static  final   String  J15CLASSPATH = "java15compile.classpath";
    private static  final   String  J16LIB = "j16lib";
    private static  final   String  J16CLASSPATH = "java16compile.classpath";
    private static  final   String  J17LIB = "j17lib";
    private static  final   String  J17CLASSPATH = "java17compile.classpath";

    private static  final   String  JDK_VENDOR = "java.vendor";
    private static  final   String  JAVA_HOME = "java.home";

    private static  final   String  JAVA_VERSION = "java.version";
    private static  final   String  OPERATING_SYSTEM = "os.name";

    private static  final   String  JDK_APPLE = "Apple ";
    private static  final   String  JDK_IBM = "IBM Corporation";
    private static  final   String  JDK_SUN = "Sun Microsystems Inc.";
    private static  final   String  JDK_ORACLE = "Oracle Corporation";

    private static  final   String  MAC_OSX = "Mac OS X";
    private static  final   String  APPLE_CLASSES_DIR = "Classes";
    private static  final   String  APPLE_COMMANDS_DIR = "Commands";
    private static  final   String  APPLE_HOME_DIR = "Home";
    private static  final   String  APPLE_LIBRARIES_DIR = "Libraries";
    private static  final   String  APPLE_RESOURCES_DIR = "Resources";

    private static  final   String  APPLE_LIB_DIR = "lib";
    private static  final   String  APPLE_JRE_DIR = "jre";
    private static  final   String  APPLE_JDK7_JRE_LIB_DIR = APPLE_JRE_DIR + "/" + APPLE_LIB_DIR;

    private static  final   String  PROPERTY_SETTER_DEBUG_FLAG = "printCompilerProperties";
    /** Property controlling extra verbose debugging information. */
    private static  final   String  PROPERTY_SETTER_VERBOSE_DEBUG_FLAG =
            "printCompilerPropertiesVerbose";
    private static boolean VERBOSE_DEBUG_ENABLED;

    /////////////////////////////////////////////////////////////////////////
    //
    //  STATE
    //
    /////////////////////////////////////////////////////////////////////////

    private Hashtable   _propertiesSnapshot;
    /** JDK vendor as reported by Java through the property 'java.vendor'. */
    private String jdkVendor;
    
    /////////////////////////////////////////////////////////////////////////
    //
    //  INNER CLASSES
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * File filter to select jar files in a directory.
     * </p>
     */
    public  static  final   class   JarFilter   implements  FileFilter
    {
        public  JarFilter() {}

        public  boolean accept( File candidate )
        {
            return candidate.getName().endsWith( ".jar" );
        }
    }
    
    /**
     * <p>
     * File filter to select child directories whose names contain a string.
     * </p>
     */
    public  static  final   class   DirContainsStringFilter   implements  FileFilter
    {
        private String  _substring;
        
        public  DirContainsStringFilter( String substring ) { _substring = substring; }

        public  boolean accept( File candidate )
        {
            if ( !candidate.isDirectory() ) { return false; }

            return ( candidate.getName().contains( _substring ) );
        }
    }
    
    /**
     * Simple class holding information about a JDK.
     * Note that the values will never be {@code null}. If a piece of
     * information is missing, {@code UNKNOWN} will be used. If the JDK home
     * variable is {@code null}, a runtime exception will be thrown.
     */
    //@Immutable
    private static final class  JDKInfo {
        /** Constant used when information is missing. */
        public static final String UNKNOWN = "unknown";
        /** The specification version of the JVM (i.e "1.6"). */
        public final String specificationVersion;
        /** The implementation version of the JVM (i.e. "1.6.0_14" or "6.0"). */
        public final String implementationVersion;
        /** The JDK implementation vendor. */
        public final String vendor;
        /** Path to the JDK home directory. */
        public final String path;

        JDKInfo(String vendor, String spec, String impl, String path) {
            this.vendor = (vendor == null ? UNKNOWN : vendor);
            this.specificationVersion = (spec == null ? UNKNOWN : spec);
            this.implementationVersion = (impl == null ? UNKNOWN : impl);
            this.path = path;
            if (path == null) {
                throw new IllegalArgumentException("JDK home cannot be null");
            }
        }

        public String toString() {
            return ("vendor=" + vendor + ", specVersion=" +
                    specificationVersion + ", implVersion=" +
                    implementationVersion + ", path=" + path);
        }
    }

    /** Holds the names of properties which define a JDK version to the build */
    private static  final   class   JDKVersion  implements Comparable
    {
        private boolean _supportsGenerics;

        /** This is the value returned by System.getProperty( "java.version" ) when you are running
         * on the JVM corresponding to this JDKVersion. This value is used to look up the JDKVersion
         * corresponding to the currently running JVM. */
        private String  _baseJavaVersion;

        /** This is the name of the lib property (e.g. "j14lib", "j15lib"). If this property is set, then we construct
         * the corresponding classpath property (e.g., "java14compile.classpath", "java15compile.classpath")
         * from the jar files in this directory */
        private String  _libPropertyName;

        /** This is the VM-specific classpath property which may be set by this program. It is used by the
         * master build script. E.g.: "java14compile.classpath", "java15compile.classpath" */
        private String  _classpathPropertyName;

        /** This is the directory name stub which we look for in trying to find directories which contain
         * Oracle JDKs. It is used by the setForMostJDKsJARInspection( ) and setForMostJDKs() methods. */
        private String  _oracleDirectoryNameSeed;

        /** This is the directory name stub which we look for in trying to find directories which contain
         * IBM JDKs. It is used by the setForMostJDKsJARInspection( ) method. */        
        private String  _ibmDirectoryNameSeed;

        /** This is another directory name stub which we look for in trying to find directories which contain
         * IBM JDKs. It is used by the setForMostJDKs( ) method. */        
        private String  _ibmDirectoryNameSeedWithoutPeriod;

        private int _sortOrder;
        //
        // The order here is important. The JDKs are listed in ascending version number order.
        // This order is used to compare them under the Comparable interface.
        //
        static  final   JDKVersion[]    ALL =
        {
            new JDKVersion( false, "1.4",  J14LIB, J14CLASSPATH, "1.4", "1.4", "142" ),
            new JDKVersion( true, "1.5", J15LIB, J15CLASSPATH, "1.5", "5.0", "50" ),
            new JDKVersion( true, "1.6", J16LIB, J16CLASSPATH, "1.6", "6.0", "60" ),
            new JDKVersion( true, "1.7", J17LIB, J17CLASSPATH, "1.7", "7.0", "70" ),
        };
        private static  int _count = 0;

        JDKVersion
            (
             boolean supportsGenerics,
             String baseJavaVersion,
             String libPropertyName,
             String classpathPropertyName,
             String oracleDirectoryNameSeed,
             String ibmDirectoryNameSeed,
             String ibmDirectoryNameSeedWithoutPeriod
             )
        {
            _sortOrder = _count++;
            
            _supportsGenerics = supportsGenerics;
            _baseJavaVersion = baseJavaVersion;
            _libPropertyName = libPropertyName;
            _classpathPropertyName = classpathPropertyName;
            _oracleDirectoryNameSeed = oracleDirectoryNameSeed;
            _ibmDirectoryNameSeed = ibmDirectoryNameSeed;
            _ibmDirectoryNameSeedWithoutPeriod = ibmDirectoryNameSeedWithoutPeriod;
        }

        /** Returns true if this version of the Java language understands generics */
        boolean supportsGenerics() { return _supportsGenerics; }

        /** Get the value of System.getProperty( "java.version" ) returned when running on this JVM */
        String  getBaseJavaVersion() { return _baseJavaVersion; }

        /** Get the name of the user-specified lib property which points at a library of jars to put on the classpath */
        String  getLibPropertyName() { return _libPropertyName; }

        /** Get the classpath property which the master build script expects us to set */
        String  getClasspathPropertyName() { return _classpathPropertyName; }

        /** Get the stub string we look for in order to find directories containing Oracle JDKs */
        String  getOracleDirectoryNameSeed() { return _oracleDirectoryNameSeed; }

        /** Get the stub string we look for in order to find directories containing IBM JDKs */
        String  getIBMDirectoryNameSeed() { return _ibmDirectoryNameSeed; }

        /** Get the stub string (without period separators) which we look for in order to find directories containing IBM JDKs */
        String  getIBMDirectoryNameSeedWithoutPeriod() { return _ibmDirectoryNameSeedWithoutPeriod; }

        /** Get the stub strings needed to find directories containing IBM JDKs. */
        static String[]    getIBMDirectoryNameSeeds()
        {
            String[]    ibmNames = new String[ ALL.length ];
            
            for ( int i = 0; i < ALL.length; i++ ) { ibmNames[ i ] = ALL[ i ].getIBMDirectoryNameSeed(); }
            
            return ibmNames;
        }
        
        /** Get the stub strings (without period separators) needed to find other directories containing IBM JDKs. */
        static String[]    getIBMDirectoryNameSeedWithoutPeriods()
        {
            String[]    ibmNames = new String[ ALL.length ];
            
            for ( int i = 0; i < ALL.length; i++ ) { ibmNames[ i ] = ALL[ i ].getIBMDirectoryNameSeedWithoutPeriod(); }
            
            return ibmNames;
        }

        /** Get the stub strings needed to find directories containing Oracle JDKs. */
        static String[]    getOracleDirectoryNameSeeds()
        {
            String[]    oracleDirectoryNameSeeds = new String[ ALL.length ];
            
            for ( int i = 0; i < ALL.length; i++ ) { oracleDirectoryNameSeeds[ i ] = ALL[ i ].getOracleDirectoryNameSeed(); }
            
            return oracleDirectoryNameSeeds;
        }

        /** Get all of the JDK versions which support generics */
        static  ArrayList<JDKVersion>   genericsSupporters()
        {
            ArrayList<JDKVersion>   retval = new ArrayList<JDKVersion>();

            for ( JDKVersion current : ALL )
            {
                if ( current.supportsGenerics() ) { retval.add( current ); }
            }

            return retval;
        }

        /** Find the JDKVersion matching the passed-in value of the java.version property */
        static  JDKVersion  matchJREversion( String jreVersion )
        {
            for ( JDKVersion current : ALL )
            {
                if ( jreVersion.startsWith( current.getBaseJavaVersion() ) ) { return current; }
            }
            
            return null;
        }

        /** Find the JDKVersion with the passed-in classpath property name */
        static  JDKVersion  matchClasspathPropertyName( String classpathPropertyName )
        {
            for ( JDKVersion current : ALL )
            {
                if ( classpathPropertyName.equals( current.getClasspathPropertyName() ) ) { return current; }
            }
            
            return null;
        }

        // Comparable implementation
        
        public  int compareTo( Object other )
        {
            if ( other ==  null ) { return 1; }
            if ( !( other instanceof JDKVersion ) ) { return -1; }
            else { return this._sortOrder - ((JDKVersion) other)._sortOrder; }
        }
        public  boolean equals( Object other ) { return ( compareTo( other ) == 0 ); }
        public  int hashCode() { return _sortOrder; }
    }

    /////////////////////////////////////////////////////////////////////////
    //
    //  CONSTRUCTORS
    //
    /////////////////////////////////////////////////////////////////////////

   /**
     * <p>
     * Let Ant conjure us out of thin air.
     * </p>
     */
    public PropertySetter()
    {}
    
    /////////////////////////////////////////////////////////////////////////
    //
    //  Task BEHAVIOR
    //
    /////////////////////////////////////////////////////////////////////////

        
   /**
     * <p>
     * Set properties based on the existing build environment. If, at the end of
     * this method, the compiler properties are still not set, then we raise an
     * error and the build aborts.
     * </p>
     */
    public  void    execute()
        throws BuildException
    {
        refreshProperties();

        debug( "\nPropertySetter environment =\n\n" + showEnvironment() + "\n\n" );

        try {
            //
            // Check for settings which are known to cause problems.
            //
            checkForProblematicSettings();

            //
            // There's nothing to do if the classpath properties are already set.
            //
            boolean isSet = true;
            for ( int i = 0; (i < JDKVersion.ALL.length) && isSet; i++ ) { isSet &= isSet( JDKVersion.ALL[ i ].getClasspathPropertyName() ); }
            if ( isSet )
            {
                debug("All required properties already set.");
                return;
            }
            
            //
            // If the library properties are set, then use them to set the
            // classpath properties.
            //
            for ( int i = 0; i < JDKVersion.ALL.length; i++ ) { tryToSetClasspathFromLib( JDKVersion.ALL[ i ] ); }

            //
            // If the library properties were not set, the following
            // logic will try to figure out how to set the
            // classpath properties based on the JDK vendor.
            //
            // This is where you plug in vendor-specific logic.
            //
            jdkVendor = getProperty(JDK_VENDOR, "");

            if (  usingMacOSXjdk( jdkVendor ) ) { setForAppleJDKs(); }
            else if ( usingIBMjdk( jdkVendor ) ) { setForIbmJDKs(); }
            else if ( usingOracleJDK( jdkVendor ) ) { setForOracleJDKs(); }
            else {
                // We don't know anything about this vendor. Print a warning
                // message and try to continue.
                echo("Unrecognized VM vendor: '" + jdkVendor + "'");
                echo("An attempt to configure the required JDKs will be made," +
                        " but the build may fail.");
                echo("In case of problems:\n" +
                        "  - consult BUILDING.html and set the required " +
                        "properties manually\n" +
                        "  - set the property printCompilerProperties to true " +
                        "and ask the Derby development community for help\n" +
                        "    (please provide the debug output from running ant)"
                        );
                setForMostJDKsJARInspection( JDKVersion.getOracleDirectoryNameSeeds() );
                setForMostJDKs( JDKVersion.getOracleDirectoryNameSeeds() );
            }
        } catch (Throwable t)
        {
            echoThrowable( t );

            if ( t instanceof BuildException) { throw (BuildException) t; }
            else { throw new BuildException( t ); }
        }

        //
        // Refresh our snapshot of the properties now that we have set
        // some additional ones.
        //
        refreshProperties();

        //
        // We now allow J14CLASSPATH to not be set. If a 1.4 JDK can't be found,
        // then the calling script will set J14CLASSPATH, based on J15CLASSPATH.
        //

        // Require at least one version which supports generics.
        requireGenerics();
    }

    /////////////////////////////////////////////////////////////////////////
    //
    //  SET PROPERTIES FOR Apple JDK
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Set the properties needed to compile using the Apple-supplied JDKs
     * </p>
     */
    private void    setForAppleJDKs()
        throws Exception
    {
        String[]    defaultLibPropertyValues = new String[ JDKVersion.ALL.length ];
        for ( int i = 0; i < JDKVersion.ALL.length; i++ )
        {
            defaultLibPropertyValues[ i ] = getProperty( JDKVersion.ALL[ i ].getLibPropertyName() );
        }

        // Obtain a list of all JDKs available to us, then specify which one to
        // use for the different versions we require.
        List<JDKInfo> jdks = locateAppleJDKs(getJdkSearchPath());
        debug("\nSelecting JDK candidates:");
        for ( int i = 0; i < JDKVersion.ALL.length; i++ )
        {
            if ( defaultLibPropertyValues[ i ] == null )
            {
                JDKVersion  currentJDK = JDKVersion.ALL[ i ];
                defaultLibPropertyValues[ i ] = getJreLib( jdks, currentJDK.getOracleDirectoryNameSeed(), jdkVendor );
            }
        }

        defaultSetter( defaultLibPropertyValues );
    }
    
    /////////////////////////////////////////////////////////////////////////
    //
    //  SET PROPERTIES FOR IBM JDKs
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Set the properties needed to compile using the IBM JDKs
     * </p>
     */
    private void    setForIbmJDKs()
        throws Exception
    {
        setForMostJDKsJARInspection( JDKVersion.getIBMDirectoryNameSeeds() );
        setForMostJDKs( JDKVersion.getIBMDirectoryNameSeedWithoutPeriods() );
    }

    /////////////////////////////////////////////////////////////////////////
    //
    //  SET PROPERTIES FOR Oracle JDKs
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Set the properties needed to compile using the Oracle JDKs. This
     * has been tested on Linux and SunOS.
     * </p>
     */
    private void setForOracleJDKs()
        throws Exception
    {
        setForMostJDKsJARInspection( JDKVersion.getOracleDirectoryNameSeeds() );
        setForMostJDKs( JDKVersion.getOracleDirectoryNameSeeds() );
    }
    
    /////////////////////////////////////////////////////////////////////////
    //
    //  JDK HEURISTICS
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * Sets the properties needed to compile using most JDKs.
     * <p>
     * Will search for JDK based on a list of root directories. A JDK is
     * identified by certain files and the content of JAR file manifests.
     */
    private void setForMostJDKsJARInspection( String[] seeds )
        throws Exception
    {
        String[]    defaultLibPropertyValues = new String[ JDKVersion.ALL.length ];
        for ( int i = 0; i < JDKVersion.ALL.length; i++ )
        {
            defaultLibPropertyValues[ i ] = getProperty( JDKVersion.ALL[ i ].getLibPropertyName() );
        }

        // Obtain a list of all JDKs available to us, then specify which one to
        // use for the different versions we require.
        List<JDKInfo> jdks = locateMostJDKs(getJdkSearchPath());
        debug("\nSelecting JDK candidates:");
        for ( int i = 0; i < JDKVersion.ALL.length; i++ )
        {
            if ( defaultLibPropertyValues[ i ] == null )
            {
                defaultLibPropertyValues[ i ] = getJreLib( jdks, seeds[ i ], jdkVendor );
            }
        }

        defaultSetter( defaultLibPropertyValues );
    }

    /**
     * <p>
     * Set the properties needed to compile using most JDKs
     * </p>
     */
    private void    setForMostJDKs( String[] seeds )
        throws Exception
    {
        List<File> jdkParents = getJdkSearchPath();

        String[]    defaultLibPropertyValues = new String[ JDKVersion.ALL.length ];
        for ( int i = 0; i < JDKVersion.ALL.length; i++ )
        {
            defaultLibPropertyValues[ i ] = getProperty( JDKVersion.ALL[ i ].getLibPropertyName() );
        }
        
        for ( int i = 0; i < JDKVersion.ALL.length; i++ )
        {
            if ( defaultLibPropertyValues[ i ] == null )
            {
                defaultLibPropertyValues[ i ] = searchForJreLib( jdkParents, seeds[ i ], false );
            }
        }

        defaultSetter( defaultLibPropertyValues );
    }

    /**
     * Search for a library directory in some likely locations.
     *
     * @param parents list of potential JDK parent directories
     * @param seed search string which identifies a given JDK version
     * @return a library directory, or <code>null</code> if not found
     */
    private String searchForJreLib(List<File> parents, String seed, boolean squawkIfEmpty) {
        for (File parent : parents) {
            String jreLib = getJreLib(parent, seed, squawkIfEmpty);
            if (jreLib != null) {
                return jreLib;
            }
        }
        return null;
    }

    /**
     * Get a list of potential JDK parent directories. These include the parent
     * directory of JAVA_HOME, and possibly some system dependent directories.
     *
     * @return a list of potential JDK parent directories
     */
    private List<File> getJdkSearchPath() throws Exception {
        ArrayList<File> searchPath = new ArrayList<File>();

        // Add parent of JAVA_HOME
        searchPath.add(getJdkParentDirectory());

        String osName = System.getProperty( OPERATING_SYSTEM );

        if ("SunOS".equals(osName)) {
            // On Solaris, JDK 1.4.2 is installed under /usr/jdk, whereas JDK
            // 5.0 and later are placed under /usr/jdk/instances. If we don't
            // find JDK 1.4.2 in the parent of JAVA_HOME, it's worth taking a
            // look at /usr/jdk before giving up.
            searchPath.add(new File("/usr/jdk"));
        }

        return searchPath;
    }

    /**
     * <p>
     * Get the parent directory of JAVA_HOME
     * </p>
     */
    private File    getJdkParentDirectory()
        throws Exception
    {
        String  javaHome = getProperty( JAVA_HOME );
        
        // slice off the jdk1.5.0_u13/jre. ant seems to tack the "/jre" onto
        // the end of what the shell thinks $JAVA_HOME is:
        
        File    javaHomeDir = new File( javaHome );
        File    ancestor = getParent( getParent( javaHomeDir ) );

        if ( ancestor == null )
        {
            echo( "JAVA_HOME directory '" + javaHome + "' does not have a grandparent directory sitting above all of the JDKs." );
        }
        verbose("jdkParent derived from '" + javaHome + "': '" +
                ancestor.getPath() + "'");
        return ancestor;
    }
    
    /**
     * <p>
     * Get a file's parent directory. Return null if there is no parent directory.
     * </p>
     */
    private File    getParent( File file )
        throws Exception
    {
        if ( file == null ) { return null; }
        if ( !file.exists() ) { return null; }
        
        return file.getParentFile();
    }


    /**
     * <p>
     * Get the path name of the library directory in the latest version of this jre
     * </p>
     */
    private String    getJreLib( File jdkParentDirectory, String jdkName, boolean squawkIfEmpty )
        throws BuildException
    {
        if ( jdkParentDirectory == null ) { return null; }
        
        File[]      versions = jdkParentDirectory.listFiles( new DirContainsStringFilter( jdkName ) );
        int         count = versions.length;

        if ( count <= 0 )
        {
            if ( squawkIfEmpty )
            { echo( "Directory '" + jdkParentDirectory.getAbsolutePath() + "' does not have any child directories containing the string '" + jdkName + "'." ); }
            
            return null;
        }

        Arrays.sort( versions );

        // Search the versions backwards (highest first) until a usable one
        // is found.
        for (int i = count - 1; i >= 0; i--) {
            File javadir = versions[i];

            if (isExcludedJDK(javadir)) {
                // This directory contains a JDK that we don't expect to
                // work. Skip it.
                continue;
            }

            String libStub = javadir.getAbsolutePath();

            //
            // If the selected java dir is a JDK rather than a JRE, then it
            // will have a jre subdirectory
            //
            File jreSubdirectory = new File(javadir, "jre");
            if (jreSubdirectory.exists()) {
                libStub = libStub + File.separator + "jre";
            }

            libStub = libStub + File.separator + "lib";

            return libStub;
        }

        return null;
    }

    /**
     * Check if the specified directory should be excluded when searching for
     * a usable set of Java libraries.
     *
     * @param dir the directory to check
     * @return {@code true} if the libraries in the directory should not be
     * used for constructing a compile classpath
     */
    private static boolean isExcludedJDK(File dir) {
        // DERBY-5189: The libraries that come with GCJ lack some classes in
        // the javax.management.remote package and cannot be used for building
        // Derby.
        return dir.getName().toLowerCase().contains("gcj");
    }

    // JDK heuristics based on inspecting JARs.
    //
    private List<JDKInfo> locateAppleJDKs(List<File> jdkParentDirectories) {
        ArrayList<JDKInfo> jdks = new ArrayList<JDKInfo>();
        if (jdkParentDirectories == null) {
            debug("WARNING: No JDK parent directories specified.");
            return jdks;
        }

        debug("\nLocating Apple JDKs:");

        final FileFilter jdkFilter = new JDKRootFileFilter();
        for (File jdkParentDirectory : jdkParentDirectories) {
            verbose("locating JDKs in '" + jdkParentDirectory + "'");
            // Limit the search to the directories in the parent directory.
            // Don't descend into sub directories.
            File[] possibleJdkRoots = jdkParentDirectory.listFiles(jdkFilter);
            for (File f : possibleJdkRoots) {
                verbose("checking root '" + f + "'");

                JDKInfo jdk = findAppleJDK6( f );               
                if ( jdk == null ) { jdk = findAppleJDK7( f ); }

                if ( jdk != null ) { jdks.add(jdk); }
            }
        }
        verbose("located " + jdks.size() + " JDKs in total");
        return jdks;
     }

    /** Find the JDKInfo for an Apple JDK at rev level 6 or earlier */
    private JDKInfo findAppleJDK6( File root )
    {
        boolean directoriesExist = requireDirectories
            (
             new File[]
             {
                 new File(root, APPLE_CLASSES_DIR),
                 new File(root, APPLE_COMMANDS_DIR),
                 new File(root, APPLE_HOME_DIR),
                 new File(root, APPLE_LIBRARIES_DIR),
                 new File(root, APPLE_RESOURCES_DIR)
             }
             );
        if ( !directoriesExist ) { return null; }
        
        File rtArchive = new File(root,new File(APPLE_CLASSES_DIR, "classes.jar").getPath());
        if (!rtArchive.exists()) {
            debug("Missing JAR: " + rtArchive);
            // Bail out, we only understand JDKs that have a
            // "Classes/classes.jar".
            return null;
        }
        // Get implementation version from the manifest.
        Manifest mf;
        try {
            JarFile rtJar = new JarFile(rtArchive);
            mf = rtJar.getManifest();
        } catch (IOException ioeIgnored) {
            // Obtaining the manifest failed for some reason.
            // If in debug mode, let the user know.
            debug("Failed to obtain manifest for " +
                  rtArchive.getAbsolutePath() + ": " +
                  ioeIgnored.getMessage());
            return null;
        }
        
        return inspectJarManifest(mf, root);
    }
    /** Return true if all of the required directories are found */
    private boolean requireDirectories( File[] requiredDirs )
    {
        for (File reqDir : requiredDirs) {
            if (!reqDir.exists()) {
                debug("Missing JDK directory: " +
                      reqDir.getAbsolutePath());
                return  false;
            }
        }
        
        return true;
    }

    /** Find the JDKInfo for an Apple JDK at rev level 7 */
    private JDKInfo findAppleJDK7( File root )
    {
        boolean directoriesExist = requireDirectories
            (
             new File[]
             {
                 new File(root, APPLE_LIB_DIR),
                 new File(root, APPLE_JRE_DIR)
             }
             );
        if ( !directoriesExist ) { return null; }
        
        File rtArchive = new File(root,new File(APPLE_JDK7_JRE_LIB_DIR, "rt.jar").getPath());
        if (!rtArchive.exists()) {
            debug("Missing JAR: " + rtArchive);
            // Bail out, we only understand JDKs that have a
            // "jre/lib/rt.jar".
            return null;
        }
        // Get implementation version from the manifest.
        Manifest mf;
        try {
            JarFile rtJar = new JarFile(rtArchive);
            mf = rtJar.getManifest();
        } catch (IOException ioeIgnored) {
            // Obtaining the manifest failed for some reason.
            // If in debug mode, let the user know.
            debug("Failed to obtain manifest for " +
                  rtArchive.getAbsolutePath() + ": " +
                  ioeIgnored.getMessage());
            return null;
        }
        
        return inspectJarManifest(mf, root);
    }

    /**
     * Searches for JDKs in the specified directories.
     *
     * @param jdkParentDirectories a list of parent directories to search in
     * @return A list containing information objects for JDKs found on the
     *      system. If no JDKs were found, the list will be empty.
     */
    private List<JDKInfo> locateMostJDKs(List<File> jdkParentDirectories) {
        ArrayList<JDKInfo> jdks = new ArrayList<JDKInfo>();
        if (jdkParentDirectories == null) {
            return jdks;
        }
        debug("\nLocating JDKs:");

        File jreLibRel = new File("jre", "lib");
        String[] jarsRelative = new String[] {
                // Special cases for IBM JDKs.
                new File(jreLibRel, "core.jar").getPath(),
                new File(jreLibRel, "vm.jar").getPath(),
                // Default JAR file to look for, used be most JDKs.
                new File(jreLibRel, "rt.jar").getPath(),
            };
        final FileFilter jdkFilter = new JDKRootFileFilter();
        for (File jdkParentDirectory : jdkParentDirectories) {
            // Limit the search to the directories in the parent directory.
            // Don't descend into sub directories.
            File[] possibleJdkRoots = jdkParentDirectory.listFiles(jdkFilter);
            for (File f : possibleJdkRoots) {
                File rtArchive = new File(f, jreLibRel.getPath());
                if (!rtArchive.exists()) {
                    // Bail out, we only understand JDKs that have a jre/lib dir
                    continue;
                }
                // Look for the various JARs that identify a JDK and see if a
                // implementation version is specified in the manifest.
                for (String jar : jarsRelative) {
                    rtArchive = new File(f, jar);
                    if (rtArchive.exists()) {
                        // Jar found.
                        Manifest mf;
                        try {
                            JarFile rtJar = new JarFile(rtArchive);
                            mf = rtJar.getManifest();
                        } catch (IOException ioeIgnored) {
                            // Obtaining the manifest failed for some reason.
                            // If in debug mode, let the user know.
                            debug("Failed to obtain manifest for " +
                                        rtArchive.getAbsolutePath() + ": " +
                                        ioeIgnored.getMessage());
                            continue;
                        }
                        JDKInfo jdk = inspectJarManifest(mf, f);
                        if (jdk != null) {
                            jdks.add(jdk);
                            break;
                        }
                    }
                    rtArchive = null; // Reset
                }
                if (rtArchive == null) {
                    // We didn't find any of the jars we were looking for, or
                    // the manifests didn't contain an implementation version.
                    // Continue with the next potential JDK root.
                    continue;
                }
            }
         }
        return jdks;
     }

    /**
     * Inspects the specified manifest to obtain information about the JDK.
     *
     * @param mf manifest from a JDK jar file
     * @param jdkHome the home directory of the JDK
     * @return An information object for the JDK, or {@code null} if no
     *      information was found.
     */
    private JDKInfo inspectJarManifest(Manifest mf, File jdkHome) {
        // The manifest may be null, as it is optional.
        if (mf == null) {
            verbose("no manifest found for JDK in '" + jdkHome + "'");
            return null;
        }
        JDKInfo info = new JDKInfo(
            mf.getMainAttributes().getValue("Implementation-Vendor"),
            mf.getMainAttributes().getValue("Specification-Version"),
            mf.getMainAttributes().getValue("Implementation-Version"),
            jdkHome.getAbsolutePath());
        debug("found JDK: " + info);
        return info;
    }

    /**
     * Returns the path to the most suitable JDK found on the system.
     * <p>
     * The selection is taken based on the specification version and potentially
     * the JDK vendor.
     *
     * @param jdks the JDKs we can choose from
     * @param specificationVersion the specification version we want, i.e.
     *      "1.4" or "1.6". {@code null} allows all valid versions.
     * @param vendor the vendor to prefer, if any
     * @return The path to the chosen JDK, or {@code null} if no suitable JDK
     *      was found.
     */
    private String getJreLib(List<JDKInfo> jdks,
            String specificationVersion, String vendor) {
        // If we have no candidate JDKs, just return null at once.
        if (jdks == null || jdks.isEmpty()) {
            debug("No candidate JDKs (version '" + specificationVersion + "')");
            return null;
        }
        ArrayList<JDKInfo> candidates = new ArrayList<JDKInfo>();
        ArrayList<String> versions = new ArrayList<String>();
        // Get the JDKs with the requested specification version.
        // Because some vendors are unable to correctly specify the meta data,
        // we have to look at the implementation version only.
        for (JDKInfo jdk : jdks) {
            String implVersion = jdk.implementationVersion;
            if (isValidVersion(implVersion, specificationVersion)) {
                candidates.add(jdk);
                if (!versions.contains(implVersion)) {
                    versions.add(implVersion);
                }
            }
        }
        // See if we found any suitable JDKs.
        if (candidates.isEmpty()) {
            debug("INFO: No valid JDK with specification " +
                        "version '" + specificationVersion + "' found");
            return null;
        }

        // Sort and reverse the version list (highest first).
        Collections.sort(versions);
        Collections.reverse(versions);

        // Try to find a JVM of the same vendor first. If that fails, return
        // the highest version suitable JDK from any vendor.
        String[] targetVendors = new String[] {
                vendor,
                null // insignificant, ignores vendor and compares version only
            };
        for (String targetVendor : targetVendors) {
            for (String version : versions) {
                for (JDKInfo jdk : candidates) {
                    if (jdk.implementationVersion.equals(version) &&
                            isSameVendor(targetVendor, jdk.vendor)) {
                        debug("Candidate JDK for specification version " +
                                specificationVersion + " (vendor " +
                                (targetVendor == null ? "ignored"
                                                      : jdkVendor) +
                                "): " + jdk);
                        return constructJreLibPath(jdk).getAbsolutePath();
                    }
                }
            }
        }
        return null;
    }

    /**
     * Constructs the path to the JRE library directory for the given JDK.
     *
     * @param jdk the target JDK
     * @return A <tt>File</tt> object pointing to the JRE library directory.
     */
    private static File constructJreLibPath(JDKInfo jdk) {
        String relLib;
        if (jdk.vendor.startsWith(JDK_APPLE)) {
            relLib = new File(APPLE_CLASSES_DIR).getPath();
        } else {
            relLib = new File("jre", "lib").getPath();
        }
        return new File(jdk.path, relLib);
    }

    /**
     * Tells if the specified implementation version is representing a valid JDK
     * version and if it satisfies the specification version.
     *
     * @param implVersion the version string to check
     * @param specVersion the specification version to satisfy
     * @return {@code true} if a valid version, {@code false} if not.
     */
    private boolean isValidVersion(String implVersion,
                                          String specVersion) {
        // Don't allow null as a version.
        if (implVersion == null) {
            debug("JDK ignored, no impl version found");
            return false;
        }
        // Don't allow early access versions.
        // This rule should at least match Sun EA versions.
        //        if ( implVersion.contains("ea") ) {
        //            debug("JDK with version '" + implVersion + "' ignored: " +
        //                    "early access");
        //            return false;
        //        }

        // See if the implementation version matches the specification version.
        if (specVersion == null) {
            return true;
        }
        // The current way of comparing the versions, is to check if the
        // specification version can be found as part of the implementation
        // version. For instance spec=1.6, matches impl=1.6.0_14.
        return implVersion.contains(specVersion);
    }

    /**
     * Tells if the two vendor names are representing the same vendor.
     *
     * @param targetVendor target vendor name, or {@code null} or whitespace /
     *      empty string if insignificant
     * @param vendor the candidate vendor name to compare with
     * @return {@code true} if considered the same or {@code targetVendor} is
     *      {@code null}, {@code false} if not.
     */
    private static boolean isSameVendor(String targetVendor, String vendor) {
        // If there is no target vendor, return true.
        if (targetVendor == null || targetVendor.trim().equals("")) {
            return true;
        }
        // If we have a target vendor, but no vendor name to compare with,
        // always return false.
        if (vendor == null || vendor.trim().equals("")) {
            return false;
        }
        // Normalize both the vendor names and compare.
        String target = normalizeVendorName(targetVendor);
        String candidate = normalizeVendorName(vendor);
        // Implement special cases here, if required.
        return candidate.equals(target);
    }

    /**
     * Normalizes the vendor name for the purpose of vendor name matching.
     *
     * @param vendorName the vendor name as reported by the VM or similar
     * @return A normalized vendor name suitable for vendor name matching.
     */
    private static String normalizeVendorName(String vendorName) {
        // Normalize the vendore names returned by Apple JDKs.
        if (vendorName.equals("Apple Inc.")) {
            // The running VM says "Apple Inc.", the JAR manifest says
            // "Apple Computer, Inc.".
            vendorName = "Apple Computer, Inc.";
        }
        // The vendor name specified in the jar file manifest differes from the
        // one return by the JVM itself for the Sun JDKs. For instance:
        //  - from JAR:        Sun Microsystems, Inc.
        //  - from running VM: Sun Microsystems Inc.
        // (http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6851869)
        return vendorName.replaceAll(",", "");
    }

    /////////////////////////////////////////////////////////////////////////
    //
    //  PROPERTY MINIONS
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Default property setter. Sets the lib properties to the passed-in values
     * and sets the classpath properties to the lists of jars in the lib
     * directories. If the library properties are already set, the already-set
     * values will override the defaults that are passed in to this method.
     * </p>
     */
    private void    defaultSetter( String[] defaultLibPropertyValues )
        throws  BuildException
    {
        if ( defaultLibPropertyValues.length != JDKVersion.ALL.length )
        {
            throw new BuildException
                (
                 "The PropertySetter program does not understand this platform. " +
                 "PropertySetter should be setting properties for " + JDKVersion.ALL.length + " JDKs. " +
                 "However, PropertySetter is instead trying to set properties for " + defaultLibPropertyValues.length + " JDKs."
                 );
        }

        for ( int i = 0; i < JDKVersion.ALL.length; i++ )
        {
            JDKVersion  currentVersion = JDKVersion.ALL[ i ];
            String  libPropertyName = currentVersion.getLibPropertyName();
            String  classpathPropertyName = currentVersion.getClasspathPropertyName();
            String  libPropertyValue = getProperty( libPropertyName, defaultLibPropertyValues[ i ] );
            
            setClasspathFromLib( classpathPropertyName, libPropertyValue, false );
        }

        // Refresh the properties snapshot to reflect the latest changes.
        refreshProperties();
    }
    
    /**
     * <p>
     * Try to set a classpath from a corresponding lib property.
     * </p>
     */
    private void    tryToSetClasspathFromLib( JDKVersion version )
        throws BuildException
    {
        String  libPropertyName = version.getLibPropertyName();
        String  classpathPropertyName = version.getClasspathPropertyName();
        String  libPropertyValue = getProperty( libPropertyName );
        
        if ( libPropertyValue != null )
        {
            debug( singleQuote( libPropertyName ) + " explicitly set to " + singleQuote( libPropertyValue ) );
            setClasspathFromLib( classpathPropertyName, libPropertyValue, true );
        }
    }
    
    /**
     * <p>
     * Set a classpath property to all of the jars in a directory.
     * If the classpath property is already set, then it is not overridden.
     * However, refuse to set certain properties if they will cause problems
     * later on.
     * Throws a BuildException if there's a problem.
     * </p>
     */
    private void    setClasspathFromLib( String classpathProperty, String libraryDirectory, boolean squawkIfEmpty )
        throws BuildException
    {
        String      classpath = getProperty( classpathProperty );

        // nothing to do if the property is already set. we can't override it.
        if ( classpath != null ) { return; }

        // refuse to set certain properties
        if ( shouldNotSetClasspathProperty( classpathProperty ) ) { return; }

        String      jars = listJars( libraryDirectory, squawkIfEmpty );

        if ( squawkIfEmpty && (jars == null) )
        {
            throw couldntSetProperty( classpathProperty );
        }

        if ( jars != null ) { setProperty( classpathProperty, jars ); }
    }

    /**
     * <p>
     * List all of the jars in a directory in lexicographical order of
     * filenames in a format suitable for using in a classpath.
     * Returns null if the directory string does not identify
     * a valid directory.
     * </p>
     */
    private String    listJars( String dirName, boolean squawkIfEmpty )
    {
        if ( dirName == null ) { return null; }

        File    dir = new File( dirName );

        if ( !dir.exists() )
        {
            if ( squawkIfEmpty) { echo( "Directory " + dirName + " does not exist." ); }
            return null;
        }
        if ( !dir.isDirectory() )
        {
            if ( squawkIfEmpty) { echo( dirName + " is not a directory." ); }
            return null;
        }

        File[]  jars = dir.listFiles( new JarFilter() );

        Arrays.sort( jars );
        // Guard against empty JDK library directories.
        // Can happen if the JDK is uninstalled when there are custom libs in
        // the jre/lib/ext directory.
        // This issue only affects the old algorithm for finding JDKs
        // (looks for specific directory names), which is used as a fallback
        // when the new algorithm (looks for specific JAR files) doesn't find
        // the required JDKs.
        if (jars.length == 0) {
            debug("INFO: Empty or invalid JDK lib directory: " + dir);
            return null;
        }

        int             count = jars.length;
        StringBuffer    buffer = new StringBuffer();

        for ( int i = 0; i < count; i++ )
        {
            if ( i > 0 ) { buffer.append( File.pathSeparatorChar ); }
            
            buffer.append( jars[ i ].getAbsolutePath() );
        }

        return buffer.toString();
    }

    /**
     * <p>
     * Return true if a property is set.
     * </p>
     */
    private boolean    isSet( String name )
    {
        String  value = getProperty( name );

        if ( value == null ) { return false; }
        else { return true; }
    }

    /**
     * <p>
     * Print a property.
     * </p>
     */
    private void    printProperty( String name )
    {
        String  value = getProperty( name );

        if ( value == null ) { value = "NULL"; }

        echo( "${" + name + "} = " + value );
    }

    /**
     * <p>
     * Gets a property. If it is not already set, defaults it to a value.
     * </p>
     */
    private String  getProperty( String name, String defaultValue )
    {
        String  value = getProperty( name );

        if ( value == null ) { value = defaultValue; }

        return value;
    }
    
    /**
     * <p>
     * Refresh the known properties.
     * </p>
     */
    private void  refreshProperties()
    {
        PropertyHelper  helper = PropertyHelper.getPropertyHelper( getProject() );
        
        _propertiesSnapshot = helper.getProperties();

        // Set the verbose debugging flag, it is used by static methods.
        VERBOSE_DEBUG_ENABLED = Boolean.valueOf((String)
                    _propertiesSnapshot.get(PROPERTY_SETTER_VERBOSE_DEBUG_FLAG)
                ).booleanValue();
    }
    
    /**
     * <p>
     * Check for settings which are known to cause problems.
     * </p>
     */
    private void  checkForProblematicSettings()
    {
        for ( JDKVersion current : JDKVersion.ALL )
        {
            String  classpathPropertyName = current.getClasspathPropertyName();
            String  libPropertyName = current.getLibPropertyName();
            String  baseJavaVersion = current.getBaseJavaVersion();

            if (
                shouldNotSetClasspathProperty( classpathPropertyName ) &&
                ( isSet( classpathPropertyName ) || isSet( libPropertyName ) )
                )
            {
                String  javaVersion = getProperty( JAVA_VERSION );
                throw new BuildException
                (
                 "\nThe build raises version mismatch errors when using a " +
                 javaVersion + " compiler with libraries from a later JDK.\n" +
                 "Please either use a " + baseJavaVersion + " (or later) compiler or do not " +
                 "set the '" +  classpathPropertyName + "' and '" + libPropertyName +
                 "' variables.\n"
                 );
            }
        }
    }
    
    /**
     * <p>
     * Returns true if the given classpath property should not be set.
     * </p>
     */
    private boolean shouldNotSetClasspathProperty( String classpathPropertyName )
    {
        //
        // A Java compiler raises version mismatch errors when used
        // with libraries supplied by a later version of Java.
        //
        String  javaVersion = getProperty( JAVA_VERSION );

        JDKVersion  compilerVersion = JDKVersion.matchJREversion( javaVersion );
        JDKVersion  propertyVersion = JDKVersion.matchClasspathPropertyName( classpathPropertyName );

        if ( compilerVersion == null )
        {
            throw new BuildException
                (
                 "\nCannot find a JDKVersion matching java.version = " + javaVersion + "\n"
                 );
        }
        if ( propertyVersion == null )
        {
            throw new BuildException
                (
                 "\nCannot find a JDKVersion matching the classpath property named = " +classpathPropertyName + "\n"
                 );
        }

        if ( compilerVersion.compareTo( propertyVersion ) < 0 ) { return true; }
        else { return false; }
    }
    
    /**
     * Return true if we are using a JDK for Mac OS X.
     */
    private static boolean usingMacOSXjdk(String jdkVendor)
    {
        return jdkVendor.startsWith( JDK_APPLE ) || MAC_OSX.equals( System.getProperty( OPERATING_SYSTEM ) );
    }

    /**
     * <p>
     * Return true if we are using an IBM JDK.
     * </p>
     */
    private boolean usingIBMjdk( String jdkVendor )
    {
        return JDK_IBM.equals( jdkVendor );
    }

    /**
     * Return true if we are using an Oracle JDK.
     */
    private static boolean usingOracleJDK(String vendor)
    {
        return JDK_SUN.equals(vendor) || JDK_ORACLE.equals(vendor);
    }

    /**
     * <p>
     * Get a property value. Returns null if the property is not set.
     * </p>
     */
    private String  getProperty( String name )
    {
        return (String) _propertiesSnapshot.get( name );
    }
    
    /**
     * <p>
     * Set an ant property.
     * </p>
     */
    private void    setProperty( String name, String value )
        throws BuildException
    {
        log( "Setting property " + name + " to " + value, Project.MSG_INFO );
        
        Property    property = new Property();

        property.setName( name );
        property.setValue( value );

        property.setProject( getProject() );
        property.execute();
    }
    
    /////////////////////////////////////////////////////////////////////////
    //
    //  GENERALLY USEFUL MINIONS
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Echo a Throwable to the console.
     * </p>
     */
    private void    echoThrowable( Throwable t )
    {
        echo ( t.getMessage() );

        StringWriter    sw = new StringWriter();
        PrintWriter     pw = new PrintWriter( sw );

        t.printStackTrace( pw );

        echo ( sw.toString() );
    }

    /**
     * <p>
     * Echo a message to the console.
     * </p>
     */
    private void    echo( String text )
    {
        log( text, Project.MSG_WARN );
    }

    /**
     * <p>
     * Require that at least one of the JDKs supports generics.
     * </p>
     */
    private void  requireGenerics()
        throws BuildException
    {
        ArrayList<JDKVersion>   genericsSupporters = JDKVersion.genericsSupporters();
        int             count = genericsSupporters.size();
        String[]    properties = new String[ count ];
        int         idx = 0;

        for ( JDKVersion version : genericsSupporters )
        {
            String  propertyName = version.getClasspathPropertyName();
            if ( getProperty( propertyName ) != null ) { return; }

            properties[ idx++ ] = propertyName;
        }

        throw couldntSetProperty( properties );
    }

    /**
     * <p>
     * Require that a property be set.
     * </p>
     */
    private void  requireProperty( String property )
        throws BuildException
    {
        if ( getProperty( property ) == null ) { throw couldntSetProperty( property ); }
    }

    /**
     * <p>
     * Object that we couldn't set some properties.
     * </p>
     */
    private BuildException  couldntSetProperty( String... properties )
    {
        StringBuffer    buffer = new StringBuffer();
        int             count = properties.length;
        
        buffer.append( "Don't know how to set " );
        for ( int i = 0; i < count; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            buffer.append( properties[ i ] );
        }
        buffer.append( " using this environment:\n\n" );
        buffer.append( showEnvironment() );
        buffer.append( "\nPlease consult BUILDING.html for instructions on how to set the compiler-classpath properties." );
        
        return new BuildException( buffer.toString() );
    }

    /**
     * <p>
     * Display the environment.
     * </p>
     */
    private String  showEnvironment()
    {
        StringBuffer    buffer = new StringBuffer();

        appendProperty( buffer, JDK_VENDOR );
        appendProperty( buffer, JAVA_HOME );
        appendProperty( buffer, JAVA_VERSION );
        appendProperty( buffer, OPERATING_SYSTEM );
        for ( int i = 0; i < JDKVersion.ALL.length; i++ )
        {
            appendProperty( buffer, JDKVersion.ALL[ i ].getLibPropertyName() );
        }
        // Build a string of the search path, which may contain multiple values.
        buffer.append("jdkSearchPath = ");
        try {
            List<File> paths = getJdkSearchPath();
            for (File path : paths) {
                buffer.append(path.getPath()).append(", ");
            }
            // Remove the trailing ", ".
            buffer.deleteCharAt(buffer.length() -1);
            buffer.deleteCharAt(buffer.length() -1);

        } catch (Exception e) {
            buffer.append("unknown (reason: ").append(e.getMessage().trim()).
                   append(")");
        }
        buffer.append("\n");

        return buffer.toString();
    }
    
    /**
     * <p>
     * Append the value of a property to an evolving string buffer.
     * </p>
     */
    private void    appendProperty( StringBuffer buffer, String propertyName )
    {
        buffer.append( propertyName );
        buffer.append( " = " );
        buffer.append( getProperty( propertyName ) );
        buffer.append( "\n" );
    }

    /**
     * Emits a debug message to the console if debugging is enabled.
     * <p>
     * Debugging is controlled by {@linkplain #PROPERTY_SETTER_DEBUG_FLAG}.
     *
     * @param msg the message to print
     */
    private void debug(CharSequence msg) {
        if (isSet(PROPERTY_SETTER_DEBUG_FLAG) ||
                VERBOSE_DEBUG_ENABLED) {
            System.out.println(msg);
        }
    }

    /**
     * Emits a debug message to the console if verbose debugging is enabled.
     * <p>
     * Verbose debugging is controlled by
     * {@linkplain #PROPERTY_SETTER_VERBOSE_DEBUG_FLAG}.
     *
     * @param msg the message to print
     */
    private static void verbose(CharSequence msg) {
        if (VERBOSE_DEBUG_ENABLED) {
            System.out.println("[verbose] " + msg);
        }
    }

    /**
     * A custom filter that accepts only directories and which in addition tries
     * to ignore duplicates (i.e. symbolic links pointing into the same
     * directory).
     */
    private static class JDKRootFileFilter
            implements FileFilter {

        private List<String> canonicalRoots = new ArrayList<String>();

        /** Accepts only directories. */
        public boolean accept(File pathname) {
            if (pathname.isDirectory()) {
                // Avoid processing the same JDK multiple times if possible.
                try {
                    String canonicalRoot = pathname.getCanonicalPath();
                    boolean accept = !canonicalRoots.contains(canonicalRoot);
                    if (accept) {
                        canonicalRoots.add(canonicalRoot);
                    }
                    verbose((accept ? "candidate" : "duplicate") + " '" +
                            pathname + "' -> '" + canonicalRoot + "'");
                    return accept;
                } catch (IOException ioe) {
                    // Ignore exception, just accept the directory.
                    verbose("file operation failed: " + ioe.getMessage());
                    return true;
                }
            }
            return false;
        }
    }

    private String  singleQuote( String raw )
    {
        return "'" + raw + "'";
    }
}
