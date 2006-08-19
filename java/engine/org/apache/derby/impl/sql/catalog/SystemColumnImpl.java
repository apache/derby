/*

   Derby - Class org.apache.derby.impl.sql.catalog.SystemColumnImpl

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

package org.apache.derby.impl.sql.catalog;

import	org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.TypeId;

/**
 * Implements the description of a column in a system table.
 *
 *
 * @version 0.1
 * @author Rick Hillegas
 */

public class SystemColumnImpl implements SystemColumn
{
	private	final String	name;
	private	final int		id;
    
    /**
     * Fully described type of the column.
     */
    private final DataTypeDescriptor type;

	/**
	 * Constructor to create a description of a column in a system table.
	 *
	 *	@param	name of column.
	 *	@param	id of column.
	 *	@param	precision of data in column.
	 *	@param	scale of data in column.
	 *	@param	nullability Whether or not column accepts nulls.
	 *	@param	dataType Datatype of column.
	 *	@param	maxLength Maximum length of data in column.
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
        
        TypeId  typeId;

        if (builtInType)
        {
            typeId = TypeId.getBuiltInTypeId(dataType);
        }
        else
        {

            typeId = TypeId.getUserDefinedTypeId(dataType, false);
        }

        this.type = new DataTypeDescriptor(
                               typeId,
                               precision,
                               scale,
                               nullability,
                               maxLength
                               );
	}

	/**
	 * Constructor to create a description of a column in a system table.
	 * This constructor is used for SQL Identifiers (varchar 128).
	 *
	 *	@param	name of column.
	 *	@param	id of column.
	 *	@param	nullability Whether or not column accepts nulls.
	 */
	public	SystemColumnImpl(	String	name,
								int		id,
								boolean	nullability)
	{
        this(name, id, 0, 0, nullability, "VARCHAR", true, 128);
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
     * Return the type of this column.
     */
    public DataTypeDescriptor getType() {
        return type;
    }
}

