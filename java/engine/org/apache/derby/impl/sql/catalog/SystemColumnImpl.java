/*

   Derby - Class org.apache.derby.impl.sql.catalog.SystemColumnImpl

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.sql.catalog;

import	org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;

/**
 * Implements the description of a column in a system table.
 *
 *
 * @version 0.1
 * @author Rick Hillegas
 */

public class SystemColumnImpl implements SystemColumn
{
	private	String	name;
	private	int		id;
	private	int		precision;
	private int		scale;
	private	boolean	nullability;
	private String	dataType;
	private boolean	builtInType;
	private	int		maxLength;

	/**
	 * Constructor to create a description of a column in a system table.
	 *
	 *	@param	Name of column.
	 *	@param	Id of column.
	 *	@param	Precision of data in column.
	 *	@param	Scale of data in column.
	 *	@param	Whether or not column accepts nulls.
	 *	@param	Datatype of column.
	 *	@param	Maximum length of data in column.
	 */
	public	SystemColumnImpl(	String	name,
								int		id,
								int		precision,
								int		scale,
								boolean	nullability,
								String	dataType,
								boolean	builtInType,
								int		maxLength )
	{
		this.name			= name;
		this.id				= id;
		this.precision		= precision;
		this.scale			= scale;
		this.nullability	= nullability;
		this.dataType		= dataType;
		this.builtInType	= builtInType;
		this.maxLength		= maxLength;
	}

	/**
	 * Constructor to create a description of a column in a system table.
	 * This constructor is used for SQL Identifiers (varchar 128).
	 *
	 *	@param	Name of column.
	 *	@param	Id of column.
	 *	@param	Whether or not column accepts nulls.
	 */
	public	SystemColumnImpl(	String	name,
								int		id,
								boolean	nullability)
	{
		this.name			= name;
		this.id				= id;
		this.nullability	= nullability;
		this.dataType		= "VARCHAR";
		this.builtInType	= true;
		this.maxLength		= 128;
	}

	/**
	 * Gets the name of this column.
	 *
	 * @return	The column name.
	 */
	public String	getName()
	{
		return	name;
	}

	/**
	 * Gets the id of this column.
	 *
	 * @return	The column id.
	 */
	public int	getID()
	{
		return	id;
	}

	/**
	 * Gets the precision of this column.
	 *
	 * @return	The precision of data stored in this column.
	 */
	public int	getPrecision()
	{
		return	precision;
	}

	/**
	 * Gets the scale of this column.
	 *
	 * @return	The scale of data stored in this column.
	 */
	public int	getScale()
	{
		return	scale;
	}

	/**
	 * Gets the nullability of this column.
	 *
	 * @return	True if this column is nullable. False otherwise.
	 */
	public boolean	getNullability()
	{
		return	nullability;
	}

	/**
	 * Gets the datatype of this column.
	 *
	 * @return	The datatype of this column.
	 */
	public String	getDataType()
	{
		return	dataType;
	}

	/**
	 * Is it a built-in type?
	 *
	 * @return	True if it's a built-in type.
	 */
	public boolean	builtInType()
	{
		return builtInType;
	}

	/**
	 * Gets the maximum length of this column.
	 *
	 * @return	The maximum length of data stored in this column.
	 */
	public int	getMaxLength()
	{
		return	maxLength;
	}

}

