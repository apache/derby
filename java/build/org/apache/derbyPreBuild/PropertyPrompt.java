/*

   Derby - Class org.apache.derbyPreBuild.PropertyPrompt

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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.PropertyHelper;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.taskdefs.Property;

/**
 * <p>
 * This is an ant Task which prompts the user for a property's value
 * and sets it if the property hasn't been set already.
 * </p>
 */
public class PropertyPrompt extends Task
{
    /////////////////////////////////////////////////////////////////////////
    //
    //  CONSTANTS
    //
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    //
    //  STATE
    //
    /////////////////////////////////////////////////////////////////////////

    private String _propertyName;
    private String _prompt;

    private Hashtable   _propertiesSnapshot;

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
    public PropertyPrompt() {}

    /////////////////////////////////////////////////////////////////////////
    //
    //  Task BEHAVIOR
    //
    /////////////////////////////////////////////////////////////////////////

        
    /** <p>Let Ant set the name of the property.</p>*/
    public void setPropertyName( String propertyName ) { _propertyName = propertyName; }

    /** <p>Let Ant set the prompt to be used in case the property isn't set.</p>*/
    public void setPrompt( String prompt ) { _prompt = prompt; }

   /**
     * <p>
     * Prompt for and set a property if it isn't already set.
     * </p>
     */
    public  void    execute()
        throws BuildException
    {
        _propertiesSnapshot = PropertyHelper.getPropertyHelper( getProject() ).getProperties();

        if ( _propertiesSnapshot.get( _propertyName ) == null  ) { promptAndSet(); }
    }
    
    /////////////////////////////////////////////////////////////////////////
    //
    //  MINIONS
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Prompt for and set the property.
     * </p>
     */
    private void promptAndSet()
        throws BuildException
    {
        try {
            String value = promptForInput( _prompt );

            setProperty( _propertyName, value );
            
        } catch (Exception e)
        {
            throw new BuildException( "Error prompting and setting property " + _propertyName + ": " + e.getMessage() );
        }

    }

    /**
     * <p>
     * Prompt the user for a line of input.
     * </p>
     */
    private String promptForInput( String promptString )
        throws IOException
    {
        log( promptString, Project.MSG_WARN );

        BufferedReader br = new BufferedReader( new InputStreamReader( System.in ) );

        return br.readLine();
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

