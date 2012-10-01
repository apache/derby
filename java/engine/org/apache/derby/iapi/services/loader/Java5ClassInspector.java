/*

   Derby - Class org.apache.derby.iapi.services.loader.Java5ClassInspector

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.iapi.services.loader;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;

/**
 * A ClassInspector for JVMs which support the language features introduced
 * by Java 5, including generics.
*/
public class Java5ClassInspector extends ClassInspector
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

	/**
		DO NOT USE! use the method in ClassFactory.
	*/
	public Java5ClassInspector( ClassFactory cf ) { super( cf ); }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Get the bounds for the type variables of a parameterized interface
     * as declared for an implementing class. A given set of type bounds could
     * be null if we're confused.
     */
    @Override
	public Class[][] getTypeBounds( Class parameterizedInterface, Class implementation )
        throws StandardException
    {
        if ( implementation == null ) { return null; }
        
        Type[]  genericInterfaces = implementation.getGenericInterfaces();
        for ( Type genericInterface : genericInterfaces )
        {
            //
            // Look for the generic interface whose raw type is the parameterized interface
            // we're interested in.
            //
            if ( genericInterface instanceof ParameterizedType )
            {
                ParameterizedType   pt = (ParameterizedType) genericInterface;
                Type    rawType = pt.getRawType();

                // found it!
                if ( parameterizedInterface == rawType )
                {
                    return findTypeBounds( pt );
                }
            }
        }

        // couldn't find the interface we're looking for. check our superclass.
        return getTypeBounds( parameterizedInterface, implementation.getSuperclass() );
    }

    /**
     * Get the type bounds for all of the type variables of the given
     * parameterized type.
     */
    private Class[][]   findTypeBounds( ParameterizedType pt )
    {
        Type[]  actualTypeArguments = pt.getActualTypeArguments();
        int     argCount = actualTypeArguments.length;
        Class[][]   retval = new Class[ argCount ][];

        for ( int i = 0; i < argCount; i++ ) { retval[ i ] = boundType( actualTypeArguments[ i ] ); }

        return retval;
    }

    /**
     * Get the bounds for a single type variable.
     */
    private Class[]    boundType( Type type )
    {
        if ( type instanceof Class )
        {
            return new Class[] { (Class) type };
        }
        else if ( type instanceof TypeVariable )
        {
            Type[]  bounds = ((TypeVariable) type).getBounds();
            int     count = bounds.length;
            Class[] retval = new Class[ count ];

            for ( int i = 0; i < count; i++ ) { retval[ i ] = getRawType( bounds[ i ] ); }

            return retval;
        }
        else { return null; }
    }

    /**
     * Get the raw type of a type bound.
     */
    private Class   getRawType( Type bound )
    {
        if ( bound instanceof Class ) { return (Class) bound; }
        else if ( bound instanceof ParameterizedType ) { return getRawType( ((ParameterizedType) bound).getRawType() ); }
        else { return null; }
    }


}

