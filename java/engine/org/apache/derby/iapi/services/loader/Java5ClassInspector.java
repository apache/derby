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

    @Override
    public Class[] getGenericParameterTypes( Class parameterizedType, Class implementation )
        throws StandardException
	{
        // construct the inheritance chain stretching from the parameterized type
        // down to the concrete implemention
        ArrayList<Class<?>>    chain = getTypeChain( parameterizedType, implementation );

        // walk the chain, filling in a map of generic types to their resolved types
        HashMap<Type,Type>  resolvedTypes = getResolvedTypes( chain );

        // compose the resolved types together in order to compute the actual
        // classes which are plugged into the variables of the parameterized type
        ArrayList<Class<?>>    parameterTypes = getParameterTypes( parameterizedType, resolvedTypes );

        // turn the list into an array
        if ( parameterTypes == null ) { return null; }

        Class[] result = new Class[ parameterTypes.size() ];
        parameterTypes.toArray( result );

        return result;
    }

    /**
     * Construct an inheritance chain of types stretching from a supertype down
     * to a concrete implementation.
     */
    private ArrayList<Class<?>>    getTypeChain( Class<?> chainEnd, Class<?> start )
    {
        ArrayList<Class<?>>    result = null;
        
        if ( start == null ) { return null; }
        if ( !chainEnd.isAssignableFrom(  start ) ) { return null; }

        if ( start == chainEnd )    { result = new ArrayList<Class<?>>(); }
        if ( result == null )
        {
            result = getTypeChain( chainEnd, start.getSuperclass() );
        
            if ( result == null )
            {
                for ( Class<?> iface : start.getInterfaces() )
                {
                    result = getTypeChain( chainEnd, iface );
                    if ( result != null ) { break; }
                }
            }
        }

        if ( result != null ) { result.add( start ); }

        return result;
    }

    /**
     * Given an inheritance chain of types, stretching from a superclass down
     * to a terminal concrete class, construct a map of generic types to their
     * resolved types.
     */
    private HashMap<Type,Type>  getResolvedTypes( ArrayList<Class<?>> chain )
    {
        if ( chain ==  null ) { return null; }
        
        HashMap<Type,Type>  resolvedTypes = new HashMap<Type,Type>();

        for ( Class<?> klass : chain )
        {
            addResolvedTypes( resolvedTypes, klass.getGenericSuperclass() );

            for ( Type iface : klass.getGenericInterfaces() )
            {
                addResolvedTypes( resolvedTypes, iface );
            }
        }

        return resolvedTypes;
    }

    /**
     * Given a generic type, add its parameter types to an evolving
     * map of resolved types. Some of the resolved types may be
     * generic type variables which will need further resolution from
     * other generic types.
     */
    private void    addResolvedTypes
        ( HashMap<Type,Type> resolvedTypes, Type genericType )
    {
        if ( genericType == null ) { return; }

        if ( genericType instanceof ParameterizedType )
        {
            ParameterizedType   pt = (ParameterizedType) genericType;
            Class       rawType = (Class) pt.getRawType();
            
            Type[] actualTypeArguments = pt.getActualTypeArguments();
            TypeVariable[] typeParameters = rawType.getTypeParameters();
            for (int i = 0; i < actualTypeArguments.length; i++)
            {
                resolvedTypes.put(typeParameters[i], actualTypeArguments[i]);
            }
        }
    }

    /**
     * Given a map of resolved types, compose them together in order
     * to resolve the actual concrete types that are plugged into the
     * parameterized type.
     */
    private ArrayList<Class<?>>    getParameterTypes
        ( Class<?> parameterizedType, HashMap<Type,Type> resolvedTypes )
    {
        if ( resolvedTypes == null ) { return null; }
        
        Type[] actualTypeArguments = parameterizedType.getTypeParameters();

        ArrayList<Class<?>> result = new ArrayList<Class<?>>();
        
        // resolve types by composing type variables.
        for (Type baseType: actualTypeArguments)
        {
            while ( resolvedTypes.containsKey( baseType ) )
            {
                baseType = resolvedTypes.get(baseType);
            }
            
            result.add( getClass( baseType ) );
        }
        
        return result;
    }

    /**
     * Get the underlying class for a type, or null if the type is a variable type.
     */
    private Class<?> getClass( Type type )
    {
        if ( type instanceof Class ) { return (Class) type; }
        else if (type instanceof ParameterizedType)
        {
            return getClass( ((ParameterizedType) type).getRawType() );
        }
        else { return null; }
    }

}

