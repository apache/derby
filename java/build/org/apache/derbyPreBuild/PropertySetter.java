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
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
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
 * <li>java18compile.classpath</li>
 * </ul>
 *
 * <p>
 * If the following library properties are set, they may influence how we set the
 * corresponding classpath properties:
 * </p>
 *
 * <ul>
 * <li>j18lib</li>
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
 * <li>Otherwise we set the classpath property corresponding to the version level of the running vm.</li>
 * </li>
 * </ul>
 */
public class PropertySetter extends Task
{
    /////////////////////////////////////////////////////////////////////////
    //
    //  CONSTANTS
    //
    /////////////////////////////////////////////////////////////////////////

    // declared in ascending order
    private static  final   VMLevel[]    VM_LEVELS =
    {
        new VMLevel( "18", "1.8" ),
    };

    private static  final   String  JDK_VENDOR = "java.vendor";
    private static  final   String  JAVA_HOME = "java.home";
    private static  final   String  JAVA_VERSION = "java.version";
    private static  final   String  OPERATING_SYSTEM = "os.name";

    private static  final   String  PROPERTY_SETTER_DEBUG_FLAG = "printCompilerProperties";
    /** Property controlling extra verbose debugging information. */
    private static  final   String  PROPERTY_SETTER_VERBOSE_DEBUG_FLAG =
            "printCompilerPropertiesVerbose";
    private static boolean VERBOSE_DEBUG_ENABLED;

    private static  final   String  FILE_TOKEN = "file:";
    private static  final   String  JAR_TERMINATOR_TOKEN = "!";
    private static  final   String  JAR_EXTENSION = ".jar";

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
     * Describes a major Java version.
     * </p>
     */
    private static  final   class   VMLevel implements  Comparable
    {
        private int _sortOrder; // used by Comparable methods
        private String  _propertySeed; // used to construct property names: java${seed}compile.classpath and j${seed}lib
        private String _javaVersionPrefix; // leading prefix of value returned by System.getProperty( "java.version" )

        private static  int _count = 0; // used to dole out sort orders
        
        public  VMLevel
            (
             String propertySeed,
             String javaVersionPrefix
             )
        {
            _sortOrder = _count++;
            _propertySeed = propertySeed;
            _javaVersionPrefix = javaVersionPrefix;
        }

        /** Get the name of the lib property  corresponding to this VMLevel */
        public  String  getLibPropertyName()
        {
            return "j" + _propertySeed + "lib";
        }

        /** Get the name of the classpath property corresponding to this VMLevel */
        public  String  getClasspathPropertyName()
        {
            return "java" + _propertySeed + "compile.classpath";
        }

        /** Returns true if this VMLevel corresponds to the the passed-in value of java.version */
        public  boolean matchesJavaVersion( String javaVersion )
        {
            return javaVersion.startsWith( _javaVersionPrefix );
        }

        // Comparable behavior
        public  int compareTo( Object other )
        {
            if ( other == null ) { return -1; }
            else if ( !(other instanceof VMLevel) ) { return -1; }
            else { return _sortOrder - ((VMLevel) other)._sortOrder; }
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
     * Set properties based on the existing build environment. If at the end of
     * this method none of the compiler properties are set, then we raise an
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
            for ( VMLevel vmLevel : VM_LEVELS )
            {
                isSet &= isSet( vmLevel.getClasspathPropertyName() );
            }
            if ( isSet )
            {
                debug("All possible properties already set.");
                return;
            }
            
            //
            // If the library properties are set, then use them to set the
            // classpath properties.
            //
            for ( VMLevel vmLevel : VM_LEVELS )
            {
                tryToSetClasspathFromLib( vmLevel );
            }

            // Refresh our property snapshot because we may have set some more by now.
            refreshProperties();

            //
            // At least set the classpath property corresponding to the currently running VM,
            // if it is not already set.
            //
            VMLevel  vmLevel = findVMLevel();
            String  currentClasspathPropertyName = vmLevel.getClasspathPropertyName();

            if ( !isSet( currentClasspathPropertyName ) )
            {
                setFromCurrentVM( vmLevel );
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

        // Require that at least one of the properties is set.
        requireAtLeastOneProperty();
    }

    /////////////////////////////////////////////////////////////////////////
    //
    //  SET FROM THE CURRENT VM
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Set the classpath property from the library shipped with the currently running VM.
     * </p>
     */
    private void    setFromCurrentVM( VMLevel vmLevel )
        throws Exception
    {
        String[]    jarDirectories =
        {
            getJarFileDirectory( "java.lang.String" ),
            getJarFileDirectory( "java.util.Vector" ),  // needed for building classes against IBM JDK 7
        };

        setClasspathFromLib( vmLevel, jarDirectories );
    }

    /**
     * <p>
     * Get the name of the directory holding the jar file which contains the passed-in class name.
     * </p>
     */
    private String  getJarFileDirectory( String className )
        throws Exception
    {
        Class   klass = Class.forName( className );
        String  fileName = klass.getName().replace( '.', '/' ) + ".class";
        ClassLoader classLoader = klass.getClassLoader();
        URL     classURL = classLoader.getSystemResource( fileName );
        String classLocation = URLDecoder.decode( classURL.toString(), System.getProperty( "file.encoding" ) );

        //
        // The classLocation looks something like this:
        //
        // jar:file:/System/Library/Java/JavaVirtualMachines/1.6.0.jdk/Contents/Classes/classes.jar!/java/lang/String.class
        //
        // To get the name of the jar file which contains the String class, we need to strip
        // off everything up to and including the "file:" token and everything starting with
        // and following the "!" token.
        //
        String jarFileName = classLocation.substring
            (
             classLocation.indexOf( FILE_TOKEN ) + FILE_TOKEN.length(),
             classLocation.indexOf( JAR_TERMINATOR_TOKEN )
             );

        File    jarFile = new File( jarFileName );
        File    jarDirectory = jarFile.getParentFile();

        return jarDirectory.getAbsolutePath();
    }
    
    /////////////////////////////////////////////////////////////////////////
    //
    //  PROPERTY MINIONS
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Try to set a classpath from a corresponding lib property.
     * </p>
     */
    private void    tryToSetClasspathFromLib( VMLevel vmLevel )
        throws BuildException
    {
        String  libPropertyName = vmLevel.getLibPropertyName();
        String  classpathPropertyName = vmLevel.getClasspathPropertyName();
        String  libPropertyValue = getProperty( libPropertyName );
        
        if ( libPropertyValue != null )
        {
            debug( singleQuote( libPropertyName ) + " explicitly set to " + singleQuote( libPropertyValue ) );
            setClasspathFromLib( vmLevel, new String[] { libPropertyValue } );
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
    private void    setClasspathFromLib( VMLevel vmLevel, String[] libraryDirectories )
        throws BuildException
    {
        String      classpathPropertyName = vmLevel.getClasspathPropertyName();
        String      classpath = getProperty( classpathPropertyName );

        // nothing to do if the property is already set. we can't override it.
        if ( classpath != null ) { return; }

        // refuse to set certain properties
        if ( shouldNotSetClasspathProperty( vmLevel ) ) { return; }

        String      jars = listJars( libraryDirectories );

        if ( jars == null )
        {
            throw couldntSetProperty( classpathPropertyName );
        }

        if ( jars != null ) { setProperty( classpathPropertyName, jars ); }
    }

    /**
     * <p>
     * List all of the jars in the passed-in directories in lexicographical order of
     * filenames in a format suitable for using in a classpath.
     * Returns null if the directory string does not identify
     * a valid directory.
     * </p>
     */
    private String    listJars( String[] dirNames )
    {
        HashSet<File>   fileSet = new HashSet<File>();

        for ( String dirName : dirNames ) { listJars( dirName, fileSet ); }

        File[]  jars = new File[ fileSet.size() ];
        fileSet.toArray( jars );
        
        Arrays.sort( jars );
        // Guard against empty JDK library directories.
        if (jars.length == 0) {
            debug("INFO: Empty or invalid JDK lib directory: " + Arrays.asList( dirNames ).toString() );
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
     * List all of the jars in the passed-in directory. Add them to
     * the evolving HashSet.
     * </p>
     */
    private void    listJars( String dirName, HashSet<File> map )
    {
        debug( "Listing jars in directory " + dirName );
        
        if ( dirName == null ) { return; }

        File    dir = new File( dirName );

        if ( !dir.exists() )
        {
            echo( "Directory " + dirName + " does not exist." );
            return;
        }
        if ( !dir.isDirectory() )
        {
            echo( dirName + " is not a directory." );
            return;
        }

        File[]  jars = dir.listFiles( new JarFilter() );

        for ( File jar : jars ) { map.add( jar ); }
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
        for ( VMLevel vmLevel : VM_LEVELS )
        {
            String  classpathPropertyName = vmLevel.getClasspathPropertyName();
            String  libPropertyName = vmLevel.getLibPropertyName();

            if (
                shouldNotSetClasspathProperty( vmLevel ) &&
                ( isSet( classpathPropertyName ) || isSet( libPropertyName ) )
                )
            {
                String  javaVersion = getProperty( JAVA_VERSION );
                throw new BuildException
                (
                 "\nThe build raises version mismatch errors when using a " +
                 javaVersion + " compiler with libraries from a later JDK.\n" +
                 "Please either use a later compiler or do not " +
                 "set the '" +  classpathPropertyName + "' and '" + libPropertyName +
                 "' variables.\n"
                 );
            }
        }
    }
    
    /**
     * <p>
     * Returns true if the corresponding classpath property should not be set.
     * </p>
     */
    private boolean shouldNotSetClasspathProperty( VMLevel candidateVMLevel )
    {
        //
        // A Java compiler raises version mismatch errors when used
        // with libraries supplied by a later version of Java.
        //

        VMLevel  compilerVMLevel = findVMLevel();

        return ( compilerVMLevel.compareTo( candidateVMLevel ) < 0 );
    }

    /**
     * Find the VM level corresponding to the java.version property.
     */
    private VMLevel  findVMLevel()   throws BuildException
    {
        String  javaVersion = getProperty( JAVA_VERSION );

        for ( VMLevel candidateLevel : VM_LEVELS )
        {
            if ( candidateLevel.matchesJavaVersion( javaVersion ) ) { return candidateLevel; }
        }

        throw new BuildException
            (
             "\nPropertySetter does not know how to handle java.version = " + javaVersion + "\n"
             );
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
     * Require that at least one of the classpath properties is set.
     * </p>
     */
    private void  requireAtLeastOneProperty()
        throws BuildException
    {
        String[]    properties = new String[ VM_LEVELS.length ];
        
        for ( int i = 0; i < VM_LEVELS.length; i++ )
        {
            String  classpathPropertyName = VM_LEVELS[ i ].getClasspathPropertyName();
            if ( isSet( classpathPropertyName ) ) { return; }

            properties[ i ] = classpathPropertyName;
        }

        throw couldntSetProperty( properties );
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
        for ( int i = 0; i < VM_LEVELS.length; i++ )
        {
            VMLevel  vmLevel = VM_LEVELS[ i ];
            appendProperty( buffer, vmLevel.getLibPropertyName() );
            appendProperty( buffer, vmLevel.getClasspathPropertyName() );
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

    private String  singleQuote( String raw )
    {
        return "'" + raw + "'";
    }
}
