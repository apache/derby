/*

   Derby - Class org.apache.derby.iapi.types.JSQLType

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

package org.apache.derby.iapi.types;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.shared.common.error.StandardException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 *	Type descriptor which wraps all 3 kinds of types supported in Derby's
 *	JSQL language: SQL types, Java primitives, Java classes.
 *
 *	This interface was originally added to support the serializing of WorkUnit
 *	signatures.
 *
 *
 */
public final class JSQLType implements Formatable
{
	///////////////////////////////////////////////////////////////////////
	//
	//	CONSTANTS
	//
	///////////////////////////////////////////////////////////////////////

	public	static	final	byte	SQLTYPE			=	0;
	public	static	final	byte	JAVA_CLASS		=	1;
	public	static	final	byte	JAVA_PRIMITIVE	=	2;

	public	static	final	byte	NOT_PRIMITIVE	=	-1;
	public	static	final	byte	BOOLEAN			=	0;
	public	static	final	byte	CHAR			=	1;
	public	static	final	byte	BYTE			=	2;
	public	static	final	byte	SHORT			=	3;
	public	static	final	byte	INT				=	4;
	public	static	final	byte	LONG			=	5;
	public	static	final	byte	FLOAT			=	6;
	public	static	final	byte	DOUBLE			=	7;

	// these two arrays are in the order of the primitive constants
	static	private	final	String[]	wrapperClassNames =
	{
		"java.lang.Boolean",
		"java.lang.Integer",	// we can't serialize char, so we convert it to int
		"java.lang.Integer",
		"java.lang.Integer",
		"java.lang.Integer",
		"java.lang.Long",
		"java.lang.Float",
		"java.lang.Double"
	};

	static	private	final	String[]	primitiveNames =
	{
		"boolean",
		"char",
		"byte",
		"short",
		"int",
		"long",
		"float",
		"double"
	};


	// here are the fields we serialize

	private	byte				category = JAVA_PRIMITIVE;
	private	DataTypeDescriptor	sqlType;
	private	String				javaClassName;
	private	byte				primitiveKind;


	///////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	///////////////////////////////////////////////////////////////////////

	/**
	  *	Public 0-arg constructor for Formatable machinery.
	  */
    public	JSQLType() { initialize( INT ); }


	/**
	  *	Create a JSQLType from a SQL type.
	  *
	  *	@param	sqlType	the SQL type to wrap
	  */
	public	JSQLType
	(
		DataTypeDescriptor	sqlType
    )
	{ initialize( sqlType ); }

	/**
	  *	Create a JSQLType given the name of a Java primitive or java class.
	  *
	  *	@param	javaName	name of java primitive or class to wrap
	  */
	public	JSQLType
	(
		String	javaName
    )
	{
		byte	primitiveID = getPrimitiveID( javaName );

		if ( primitiveID != NOT_PRIMITIVE ) { initialize( primitiveID ); }
		else { initialize( javaName ); }
	}

	/**
	  *	Create a JSQLType for a Java primitive.
	  *
	  *	@param	primitiveKind	primitive to wrap
	  */
	public	JSQLType
	(
		byte	primitiveKind
    )
	{ initialize( primitiveKind ); }

	/**
	  *	What kind of type is this:
	  *
	  *	@return	one of the following: SQLTYPE, JAVA_PRIMITIVE, JAVA_CLASS
	  */
    public	byte	getCategory() { return category; }

	/**
	  *	If this is a JAVA_PRIMITIVE, what is its name?
	  *
	  *	@return	BOOLEAN, INT, ... if this is a JAVA_PRIMITIVE.
	  *				NOT_PRIMITIVE if this is SQLTYPE or JAVA_CLASS.
	  */
    public	byte	getPrimitiveKind() { return primitiveKind; }

	/**
	  *	If this is a JAVA_CLASS, what is it's name?
	  *
	  *	@return	java class name if this is a JAVA_CLASS
	  *				null if this is SQLTYPE or JAVA_PRIMITIVE
	  */
    public	String	getJavaClassName() { return javaClassName; }

	/**
	  *	What's our SQLTYPE?
	  *
	  *	@return	the DataTypeDescriptor corresponding to this type
	  *
	  */
	public	DataTypeDescriptor	getSQLType
	(
    )
//IC see: https://issues.apache.org/jira/browse/DERBY-4484
        throws StandardException
	{
		// might not be filled in if this is a JAVA_CLASS or JAVA_PRIMITIVE
		if ( sqlType == null )
		{
			String	className;

			if ( category == JAVA_CLASS )
			{
				className = javaClassName;
			}
			else
			{
				className = getWrapperClassName( primitiveKind );
			}

			sqlType = DataTypeDescriptor.getSQLDataTypeDescriptor( className );
		}

		return sqlType;
	}

    // Give read-only access to array of strings
	public static String getPrimitiveName(byte index){
//IC see: https://issues.apache.org/jira/browse/DERBY-4293
	    return primitiveNames[index];
	}

	///////////////////////////////////////////////////////////////////////
	//
	//	Formatable BEHAVIOR
	//
	///////////////////////////////////////////////////////////////////////

	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.JSQLTYPEIMPL_ID; }

	/**
	  @see java.io.Externalizable#readExternal
	  @exception IOException thrown on error
	  @exception ClassNotFoundException	thrown on error
	  */
	public void readExternal( ObjectInput in )
		 throws IOException, ClassNotFoundException
	{
		byte	frozenCategory = in.readByte();

		switch ( frozenCategory )
		{
		    case SQLTYPE:

				initialize( (DataTypeDescriptor) in.readObject() );
				break;

		    case JAVA_CLASS:

				initialize( (String) in.readObject() );
				break;

		    case JAVA_PRIMITIVE:

				initialize( in.readByte() );
				break;
		}
	}

	/**

	  @exception IOException thrown on error
	  */
	public void writeExternal( ObjectOutput out )
		 throws IOException
	{
		out.writeByte( category );

		switch ( category )
		{
		    case SQLTYPE:

				out.writeObject( sqlType );
				break;

		    case JAVA_CLASS:

				out.writeObject( javaClassName );
				break;

		    case JAVA_PRIMITIVE:

				out.writeByte( primitiveKind );
				break;

		}
	}


	///////////////////////////////////////////////////////////////////////
	//
	//	INITIALIZATION MINIONS
	//
	///////////////////////////////////////////////////////////////////////

	private	void	initialize( byte primitiveKind )
	{ initialize( JAVA_PRIMITIVE, null, null, primitiveKind ); }

	private	void	initialize( DataTypeDescriptor sqlType )
	{ initialize( SQLTYPE, sqlType, null, NOT_PRIMITIVE ); }

	private	void	initialize( String javaClassName )
	{ initialize( JAVA_CLASS, null, javaClassName, NOT_PRIMITIVE ); }

	/**
	  *	Initialize this JSQL type. Minion of all constructors.
	  *
	  *	@param	category		SQLTYPE, JAVA_CLASS, JAVA_PRIMITIVE
	  *	@param	sqlType			corresponding SQL type if category=SQLTYPE
	  *	@param	javaClassName	corresponding java class if category=JAVA_CLASS
	  *	@param	primitiveKind	kind of primitive if category=JAVA_PRIMITIVE
	  */
	private	void	initialize
	(
		byte				category,
		DataTypeDescriptor	sqlType,
		String				javaClassName,
		byte				primitiveKind
    )
	{
		this.category = category;
		this.sqlType = sqlType;
		this.javaClassName = javaClassName;
		this.primitiveKind = primitiveKind;

	}
	 

	///////////////////////////////////////////////////////////////////////
	//
	//	GENERAL MINIONS
	//
	///////////////////////////////////////////////////////////////////////

	/**
	  *	Gets the name of the java wrapper class corresponding to a primitive.
	  *
	  *	@param	primitive	BOOLEAN, INT, ... etc.
	  *
	  *	@return	name of the java wrapper class corresponding to the primitive
	  */
	public	static String	getWrapperClassName
	(
		byte	primitive
    )
	{
		if ( primitive == NOT_PRIMITIVE ) { return ""; }
		return wrapperClassNames[ primitive ];
	}


	/**
	  *	Translate the name of a java primitive to an id
	  *
	  *	@param	name	name of primitive
	  *
	  *	@return	BOOLEAN, INT, ... etc if the name is that of a primitive.
	  *			NOT_PRIMITIVE otherwise
	  */
	public	static byte	getPrimitiveID
	(
		String	name
    )
	{
		for ( byte ictr = BOOLEAN; ictr <= DOUBLE; ictr++ )
		{
			if ( primitiveNames[ ictr ].equals( name ) ) { return ictr; }
		}

		return	NOT_PRIMITIVE;
	}


}
