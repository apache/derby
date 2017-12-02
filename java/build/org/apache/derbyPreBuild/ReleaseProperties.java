/*

   Derby - Class org.apache.derbyPreBuild.ReleaseProperties

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
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Calendar;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.taskdefs.Property;
import org.apache.tools.ant.Task;

/**
 * <p>
 * This ant task creates the release properties needed to define the release id
 * when building the Derby distributions. For a description of the Derby release id,
 * see http://db.apache.org/derby/papers/versionupgrade.html
 * </p>
 *
 * <p>
 * This task also sets a property for use by downstream targets during
 * the release-build:
 * </p>
 *
 * <ul>
 * <li><b>derby.release.id.new</b> - The new id for the branch, in case we were asked to bump the release id.</li>
 * </ul>
 */

public class ReleaseProperties extends Task
{
    /////////////////////////////////////////////////////////////////////////
    //
    //  CONSTANTS
    //
    /////////////////////////////////////////////////////////////////////////
    private static final String LS = System.getProperty("line.separator");
    private static final String APACHE_LICENSE_HEADER =
        "# Licensed to the Apache Software Foundation (ASF) under one or more" + LS +
        "# contributor license agreements.  See the NOTICE file distributed with" + LS +
        "# this work for additional information regarding copyright ownership." + LS +
        "# The ASF licenses this file to you under the Apache License, Version 2.0" + LS +
        "# (the \"License\"); you may not use this file except in compliance with" + LS +
        "# the License.  You may obtain a copy of the License at" + LS +
        "#" + LS +
        "#     http://www.apache.org/licenses/LICENSE-2.0" + LS +
        "#" + LS +
        "# Unless required by applicable law or agreed to in writing, software" + LS +
        "# distributed under the License is distributed on an \"AS IS\" BASIS," + LS +
        "# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied." + LS +
        "# See the License for the specific language governing permissions and" + LS +
        "# limitations under the License." + LS + LS;
    
	public final static int	MAINT_ENCODING = org.apache.derby.iapi.services.info.PropertyNames.MAINT_ENCODING;
    private final static int MAINT_LENGTH = 7;

    private final   static  String  DRDA_MAINT = "drdamaint";
    private final   static  int DRDA_MAINT_ID_DEFAULT = 0;

    // properties to set on the way out
    private static final String NEW_RELEASE_ID = "derby.release.id.new";

    /////////////////////////////////////////////////////////////////////////
    //
    //  STATE
    //
    /////////////////////////////////////////////////////////////////////////

    // set by caller. Derby release id of the form "N.N.N.N" or "N.N.N.N beta"
    private String _releaseID;

    // set by caller. name of file where release properties will be written
    private String _releasePropertiesFileName;
    
    // set by caller. true if the last digit of the release id should be bumped.
    private boolean _bump;
    
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
    public ReleaseProperties()
    {}
    
    /////////////////////////////////////////////////////////////////////////
    //
    //  Task BEHAVIOR
    //
    /////////////////////////////////////////////////////////////////////////

        
    /** <p>Let Ant set the Derby release id, a string of the form N.N.N.N.</p>*/
    public void setReleaseID( String releaseID ) { _releaseID = releaseID; }

    /** <p>Let Ant set the output file name.</p>*/
    public void setReleasePropertiesFileName( String fileName ) { _releasePropertiesFileName = fileName; }

    /** <p>Let Ant set our bumping behavior to true or false.</p>*/
    public void setBump( String bumpFlag ) { _bump = Boolean.parseBoolean( bumpFlag ); }

   /**
     * <p>
     * Create the release properties file from the release id. Sets the
     * property derby.release.id.new equal to the resulting release id.
     * </p>
     */
    public  void    execute()
        throws BuildException
    {
        File                 target = new File( _releasePropertiesFileName );
        FileWriter      propertiesFW = null;
        PrintWriter    propertiesPW = null;

        try {
            int     drdaMaintID = readDRDAMaintID( target );
            System.out.println( "XXX ReleaseProperties. drda maint id = " + drdaMaintID );
            
            VersionID versionID = new VersionID( _releaseID );
            if ( _bump ) { versionID.bump(); }
            
            int major = versionID.getMajor();
            int minor = versionID.getMinor();
            int currentYear = getCurrentYear();

            propertiesFW = new FileWriter( target );
            propertiesPW = new PrintWriter( propertiesFW );

            propertiesPW.println( APACHE_LICENSE_HEADER );

            propertiesPW.println( DRDA_MAINT + "=" + drdaMaintID );
            propertiesPW.println( "maint=" + encodeFixpackAndPoint( versionID ) );
            propertiesPW.println( "major=" + major );
            propertiesPW.println( "minor=" + minor );
            propertiesPW.println( "eversion=" + versionID.getBranchName() );
            propertiesPW.println( "beta=" + versionID.isBeta() );
            propertiesPW.println( "copyright.comment=Copyright 1997, " + currentYear + " The Apache Software Foundation or its licensors, as applicable." );
            propertiesPW.println( "vendor=The Apache Software Foundation" ) ;
            propertiesPW.println( "copyright.year=" + currentYear ) ;
            propertiesPW.println( "release.id.long=" + versionID.toString() ) ;

            setProperty( NEW_RELEASE_ID, versionID.toString() );
        }
        catch (Exception e)
        {
            throw new BuildException( "Could not generate release properties: " + e.getMessage(), e );
        }
        finally
        {
            try {
                finishWriting( propertiesFW, propertiesPW );
            }
            catch (Exception ex)
            {
                throw new BuildException( "Error closing file writers.", ex );
            }
        }
    }
    
    /////////////////////////////////////////////////////////////////////////
    //
    //  MINIONS
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Stuff the third and fourth numbers of a Derby release id into the
     * encoded format expected by the Derby release machinery.
     * </p>
     */
    private String encodeFixpackAndPoint( VersionID versionID )
    {
        int result = ( versionID.getFixpack() * MAINT_ENCODING ) + versionID.getPoint();

        // the convention is to represent the number as 7 digits even
        // if the number is 0
        String retval = Integer.toString( result );
        int  length = retval.length();

        int count = MAINT_LENGTH - length;
        for ( int i = 0; i < count; i++ ) { retval = "0" + retval; }

        return retval;
    }

    /**
     * <p>
     * Get the current year as an int.
     * </p>
     */
    private int getCurrentYear()
    {
        return Calendar.getInstance().get( Calendar.YEAR );
    }
    
    /**
     * <p>
     * Flush and close file writers.
     * </p>
     */
    private void    finishWriting( FileWriter fw, PrintWriter pw )
        throws IOException
    {
        if ( (fw == null) || (pw == null) ) { return; }
        
        pw.flush();
        fw.flush();

        pw.close();
        fw.close();
    }

    /**
     * <p>
     * Read the DRDA maintenance id from the existing release properties.
     * Returns 0 if the release properties file doesn't exist.
     * </p>
     */
    private int readDRDAMaintID( File inputFile )
        throws Exception
    {
        if ( !inputFile.exists() ) { return DRDA_MAINT_ID_DEFAULT; }
        
        Properties  releaseProperties = new Properties();
        releaseProperties.load( new FileInputStream( inputFile ) );

        String  stringValue = releaseProperties.getProperty( DRDA_MAINT );

        return Integer.parseInt( stringValue );
    }
    
    /////////////////////////////////////////////////////////////////////////
    //
    //  INNER CLASSES
    //
    /////////////////////////////////////////////////////////////////////////

    public static final class VersionID
    {
        private int _major;
        private int _minor;
        private int _fixpack;
        private int _point;
        private boolean _isBeta = false;

        public VersionID( String text )
            throws BuildException
        {
            StringTokenizer tokenizer = new StringTokenizer( text, ". " );

            try {
                _major = Integer.parseInt( tokenizer.nextToken() );
                _minor = Integer.parseInt( tokenizer.nextToken() );
                _fixpack = Integer.parseInt( tokenizer.nextToken() );
                _point = Integer.parseInt( tokenizer.nextToken() );

                if ( tokenizer.hasMoreTokens() )
                {
                    if ( tokenizer.nextToken().trim().toLowerCase().equals( "beta" ) )
                    { _isBeta = true; }
                    else { throw new Exception( "Illegal trailing token" ); }
                }
            }
            catch (Exception e) { throw badID( text ); }
        }

        /** Bump the last digit of the release id */
        public void bump() { _point++; }

        public int getMajor() { return _major; }
        public int getMinor() { return _minor; }
        public int getFixpack() { return _fixpack; }
        public int getPoint() { return _point; }
        public boolean isBeta() { return _isBeta; }

        public String getBranchName() { return Integer.toString( _major ) + '.' + Integer.toString( _minor ); }

        public String toString()
        {
            StringBuffer buffer = new StringBuffer();

            buffer.append( _major ); buffer.append( '.' );
            buffer.append( _minor ); buffer.append( '.' );
            buffer.append( _fixpack ); buffer.append( '.' );
            buffer.append( _point );

            if ( _isBeta ) { buffer.append( " beta" ); }

            return buffer.toString();
        }

        private BuildException badID( String text )
        {
            return new BuildException( "Version id \"" + text + "\" is not a string of the form \"N.N.N.N\" or \"N.N.N.N beta\"" );
        }
    }

    /**
     * <p>
     * Set an ant property.
     * </p>
     */
    private void    setProperty( String name, String value )
        throws BuildException
    {
        Property    property = new Property();

        property.setName( name );
        property.setValue( value );

        property.setProject( getProject() );
        property.execute();
    }

}

