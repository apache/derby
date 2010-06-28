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

    private static  final   String  JDK_VENDOR = "java.vendor";
    private static  final   String  JAVA_HOME = "java.home";

    private static  final   String  JAVA_VERSION = "java.version";
    private static  final   String  OPERATING_SYSTEM = "os.name";

    private static  final   String  JDK_APPLE = "Apple ";
    private static  final   String  JDK_IBM = "IBM Corporation";
    private static  final   String  JDK_SUN = "Sun Microsystems Inc.";

    private static  final   String  APPLE_JAVA_ROOT = "/System/Library/Frameworks/JavaVM.framework/Versions";
    private static  final   String  APPLE_CLASSES_DIR = "Classes";
    private static  final   String  APPLE_COMMANDS_DIR = "Commands";
    private static  final   String  APPLE_HEADERS_DIR = "Headers";
    private static  final   String  APPLE_HOME_DIR = "Home";
    private static  final   String  APPLE_LIB_DIR = "Libraries";
    private static  final   String  APPLE_RESOURCES_DIR = "Resources";

    private static  final   String  JAVA_5 = "1.5";

    private static  final   String  PROPERTY_SETTER_DEBUG_FLAG = "printCompilerProperties";

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
            if ( isSet( J14CLASSPATH ) && isSet( J15CLASSPATH ) &&
                    isSet( J16CLASSPATH ) ) {
                debug("All required properties already set.");
                return;
            }
            
            //
            // If the library properties are set, then use them to set the
            // classpath properties.
            //
            String  j14lib = getProperty( J14LIB );
            String  j15lib = getProperty( J15LIB );
            String  j16lib = getProperty( J16LIB );

            if ( j14lib != null ) {
                debug("'j14lib' explicitly set to '" + j14lib + "'");
                setClasspathFromLib(J14CLASSPATH, j14lib, true );
            }
            if ( j15lib != null ) {
                debug("'j15lib' explicitly set to '" + j15lib + "'");
                setClasspathFromLib(J15CLASSPATH, j15lib, true );
            }
            if ( j16lib != null ) {
                debug("'j16lib' explicitly set to '" + j16lib + "'");
                setClasspathFromLib(J16CLASSPATH, j16lib, true );
            }

            //
            // If the library properties were not set, the following
            // logic will try to figure out how to set the
            // classpath properties based on the JDK vendor.
            //
            // This is where you plug in vendor-specific logic.
            //
            jdkVendor = getProperty(JDK_VENDOR, "");

            if (  jdkVendor.startsWith( JDK_APPLE ) ) { setForAppleJDKs(); }
            else if ( usingIBMjdk( jdkVendor ) ) { setForIbmJDKs(); }
            else if ( JDK_SUN.equals( jdkVendor ) ) { setForSunJDKs(); }
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
                setForMostJDKsJARInspection("1.4", "1.5", "1.6");
                setForMostJDKs("1.4", "1.5", "1.6");
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

        // Require that at least one of these be set now.
        requireAtLeastOneProperty( J15CLASSPATH, J16CLASSPATH );
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
        String  default_j14lib = getProperty( J14LIB );
        String  default_j15lib = getProperty( J15LIB );
        String  default_j16lib = getProperty( J16LIB );

        // Obtain a list of all JDKs available to us, then specify which one to
        // use for the different versions we require.
        List<JDKInfo> jdks = locateAppleJDKs(getJdkSearchPath());
        debug("\nSelecting JDK candidates:");
        if (default_j14lib == null) {
            default_j14lib = getJreLib(jdks, "1.4", jdkVendor);
        }
        if (default_j15lib == null) {
            default_j15lib = getJreLib(jdks, "1.5", jdkVendor);
        }
        if (default_j16lib == null) {
            default_j16lib = getJreLib(jdks, "1.6", jdkVendor);
        }

        defaultSetter(default_j14lib, default_j15lib, default_j16lib);
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
        setForMostJDKsJARInspection("1.4", "5.0", "6.0");
        setForMostJDKs( "142", "50", "60" );
    }
    
    /////////////////////////////////////////////////////////////////////////
    //
    //  SET PROPERTIES FOR Sun JDKs
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Set the properties needed to compile using the Sun JDKs. This
     * has been tested on Linux and SunOS.
     * </p>
     */
    private void    setForSunJDKs()
        throws Exception
    {
        setForMostJDKsJARInspection("1.4", "1.5", "1.6");
        setForMostJDKs( "1.4", "1.5", "1.6" );
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
    private void setForMostJDKsJARInspection(
            String seed14, String seed15, String seed16)
        throws Exception {
        String  default_j14lib = getProperty( J14LIB );
        String  default_j15lib = getProperty( J15LIB );
        String  default_j16lib = getProperty( J16LIB );

        // Obtain a list of all JDKs available to us, then specify which one to
        // use for the different versions we require.
        List<JDKInfo> jdks = locateMostJDKs(getJdkSearchPath());
        debug("\nSelecting JDK candidates:");
        if (default_j14lib == null) {
            default_j14lib = getJreLib(jdks, seed14, jdkVendor);
        }
        if (default_j15lib == null) {
            default_j15lib = getJreLib(jdks, seed15, jdkVendor);
        }
        if (default_j16lib == null) {
            default_j16lib = getJreLib(jdks, seed16, jdkVendor);
        }

        defaultSetter(default_j14lib, default_j15lib, default_j16lib);
    }

    /**
     * <p>
     * Set the properties needed to compile using most JDKs
     * </p>
     */
    private void    setForMostJDKs( String seed14, String seed15, String seed16 )
        throws Exception
    {
        List<File> jdkParents = getJdkSearchPath();

        String  default_j14lib = getProperty( J14LIB );
        String  default_j15lib = getProperty( J15LIB );
        String  default_j16lib = getProperty( J16LIB );
        
        if ( default_j14lib == null )
        { default_j14lib = searchForJreLib(jdkParents, seed14, false ); }

        if ( default_j15lib == null )
        { default_j15lib = searchForJreLib(jdkParents, seed15, false ); }

        if ( default_j16lib == null )
        { default_j16lib = searchForJreLib(jdkParents, seed16, false ); }

        defaultSetter( default_j14lib, default_j15lib, default_j16lib );
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

        String osName = System.getProperty("os.name");

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

        File        javadir = versions[ count - 1 ];
        String      libStub = "";

        //
        // If the selected java dir is a JDK rather than a JRE, then it
        // will have a jre subdirectory
        //
        File        jreSubdirectory = new File( javadir, "jre" );
        if ( jreSubdirectory.exists() ) { libStub = libStub + File.separator + "jre"; }

        libStub = libStub + File.separator + "lib";

        return javadir.getAbsolutePath() + libStub;
    }

    // JDK heuristics based on inspecting JARs.
    //
    private List<JDKInfo> locateAppleJDKs(List<File> jdkParentDirectories) {
        ArrayList<JDKInfo> jdks = new ArrayList<JDKInfo>();
        if (jdkParentDirectories == null) {
            return jdks;
        }

        debug("\nLocating JDKs:");

        final FileFilter jdkFilter = new JDKRootFileFilter();
        for (File jdkParentDirectory : jdkParentDirectories) {
            // Limit the search to the directories in the parent directory.
            // Don't descend into sub directories.
            File[] possibleJdkRoots = jdkParentDirectory.listFiles(jdkFilter);
            for (File f : possibleJdkRoots) {

                File[] requiredDirs = new File[] {
                    new File(f, APPLE_CLASSES_DIR),
                    new File(f, APPLE_COMMANDS_DIR),
                    new File(f, APPLE_HEADERS_DIR),
                    new File(f, APPLE_HOME_DIR),
                    new File(f, APPLE_LIB_DIR),
                    new File(f, APPLE_RESOURCES_DIR)
                };
                
                boolean dirsOK = true;
                for (File reqDir : requiredDirs) {
                    if (!reqDir.exists()) {
                        debug("Missing JDK directory: " +
                                reqDir.getAbsolutePath());
                        dirsOK = false;
                        break;
                    }
                }
                if (!dirsOK) {
                    continue;
                }

                File rtArchive = new File(f,
                        new File(APPLE_CLASSES_DIR, "classes.jar").getPath());
                if (!rtArchive.exists()) {
                    debug("Missing JAR: " + rtArchive);
                    // Bail out, we only understand JDKs that have a
                    // "Classes/classes.jar".
                    continue;
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
                    continue;
                }
                JDKInfo jdk = inspectJarManifest(mf, f);
                if (jdk != null) {
                    jdks.add(jdk);
                    continue;
                }
            }
        }
        return jdks;
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
        if (implVersion.contains("ea")) {
            debug("JDK with version '" + implVersion + "' ignored: " +
                    "early access");
            return false;
        }

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
    private void    defaultSetter( String default_j14lib, String default_j15lib, String default_j16lib )
        throws  BuildException
    {
        String  j14lib = getProperty( J14LIB, default_j14lib );
        String  j15lib = getProperty( J15LIB, default_j15lib );
        String  j16lib = getProperty( J16LIB, default_j16lib );

        setClasspathFromLib( J14CLASSPATH, j14lib, false );
        setClasspathFromLib( J15CLASSPATH, j15lib, false );
        setClasspathFromLib( J16CLASSPATH, j16lib, false );

        // Refresh the properties snapshot to reflect the latest changes.
        refreshProperties();
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
        if ( shouldNotSet( classpathProperty ) ) { return; }

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
    }
    
    /**
     * <p>
     * Check for settings which are known to cause problems.
     * </p>
     */
    private void  checkForProblematicSettings()
    {
        if (
            shouldNotSet( J16CLASSPATH ) &&
            ( isSet( J16CLASSPATH ) || isSet( J16LIB ) )
           )
        {
            throw new BuildException
                (
                 "\nThe build raises version mismatch errors when using a " +
                 "Java 5 compiler with Java 6 libraries.\n" +
                 "Please either use a Java 6 (or later) compiler or do not " +
                 "set the '" +  J16CLASSPATH + "' and '" + J16LIB +
                 "' variables.\n"
                 );
        }

    }
    
    /**
     * <p>
     * Returns true if the given property should not be set.
     * </p>
     */
    private boolean shouldNotSet( String property )
    {
        //
        // A Java 5 compiler raises version mismatch errors when used
        // with Java 6 libraries.
        //
        String  javaVersion = getProperty( JAVA_VERSION );
        
        return ( javaVersion.startsWith( JAVA_5 ) &&
                    J16CLASSPATH.equals( property  ) );
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
     * Require that at least one of the passed in properties be set.
     * </p>
     */
    private void  requireAtLeastOneProperty( String... properties )
        throws BuildException
    {
        int             count = properties.length;

        for ( String property : properties )
        {
            if ( getProperty( property ) != null ) { return; }
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
        appendProperty( buffer, J14LIB );
        appendProperty( buffer, J15LIB );
        appendProperty( buffer, J16LIB );
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
        if (isSet(PROPERTY_SETTER_DEBUG_FLAG)) {
            System.out.println(msg);
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
                    return accept;
                } catch (IOException ioe) {
                    // Ignore exception, just accept the directory.
                    return true;
                }
            }
            return false;
        }
    }
}
