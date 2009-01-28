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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;

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
 * <li>Otherwise, if we don't recognize the vm vendor, we abort the build.</li>
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

    private static  final   String  JAVA_5 = "1.5";

    private static  final   String  PROPERTY_SETTER_DEBUG_FLAG = "printCompilerProperties";

    /////////////////////////////////////////////////////////////////////////
    //
    //  STATE
    //
    /////////////////////////////////////////////////////////////////////////

    private Hashtable   _propertiesSnapshot;
    
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

        if ( isSet( PROPERTY_SETTER_DEBUG_FLAG ) )
        {
            echo( "\nPropertySetter environment =\n\n" + showEnvironment() + "\n\n" );
        }

        try {
            //
            // Check for settings which are known to cause problems.
            //
            checkForProblematicSettings();
            
            //
            // There's nothing to do if the classpath properties are already set.
            //
            if ( isSet( J14CLASSPATH ) && isSet( J15CLASSPATH ) && isSet( J16CLASSPATH ) ) { return; }
            
            //
            // If the library properties are set, then use them to set the
            // classpath properties.
            //
            String  j14lib = getProperty( J14LIB );
            String  j15lib = getProperty( J15LIB );
            String  j16lib = getProperty( J16LIB );

            if ( j14lib != null ) { setClasspathFromLib(J14CLASSPATH, j14lib, true ); }
            if ( j15lib != null ) { setClasspathFromLib(J15CLASSPATH, j15lib, true ); }
            if ( j16lib != null ) { setClasspathFromLib(J16CLASSPATH, j16lib, true ); }

            //
            // If the library properties were not set, the following
            // logic will try to figure out how to set the
            // classpath properties based on the JDK vendor.
            //
            // This is where you plug in vendor-specific logic.
            //
            String  jdkVendor = getProperty( JDK_VENDOR );

            if ( jdkVendor == null ) { jdkVendor = ""; }

            if (  jdkVendor.startsWith( JDK_APPLE ) ) { setForAppleJDKs(); }
            else if ( usingIBMjdk( jdkVendor ) ) { setForIbmJDKs(); }
            else if ( JDK_SUN.equals( jdkVendor ) ) { setForSunJDKs(); }
            
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
        throws BuildException
    {
        defaultSetter( APPLE_JAVA_ROOT + "/1.4/Classes", APPLE_JAVA_ROOT + "/1.5/Classes", APPLE_JAVA_ROOT + "/1.6/Classes" );
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
        setForMostJDKs( "1.4.", "1.5.", "1.6" );
    }
    
    /////////////////////////////////////////////////////////////////////////
    //
    //  JDK HEURISTICS
    //
    /////////////////////////////////////////////////////////////////////////

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
                 "\nThe build raises version mismatch errors when using the IBM Java 5 compiler with Java 6 libraries.\n" +
                 "Please either use a Java 6 (or later) compiler or do not set the '" +  J16CLASSPATH + "' and '" + J16LIB + "' variables.\n"
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
        // The IBM Java 5 compiler raises version mismatch errors when used
        // with the IBM Java 6 libraries.
        //
        String  jdkVendor = getProperty( JDK_VENDOR );
        String  javaVersion = getProperty( JAVA_VERSION );
        
        return ( usingIBMjdk( jdkVendor ) && javaVersion.startsWith( JAVA_5 ) &&  J16CLASSPATH.equals( property  ) );
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

}

