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

import java.sql.Types;

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

class SystemColumnImpl implements SystemColumn
{
	private	final String	name;
   
    /**
     * Fully described type of the column.
     */
    private final DataTypeDescriptor type;
    
    /**
     * Create a system column for a builtin type.
     * 
     * @param name
     *            name of column
     * @param jdbcTypeId
     *            JDBC type id from java.sql.Types
     * @param nullability
     *            Whether or not column accepts nulls.
     */
    static SystemColumn getColumn(String name, int jdbcTypeId,
            boolean nullability) {
        return new SystemColumnImpl(name, DataTypeDescriptor
                .getBuiltInDataTypeDescriptor(jdbcTypeId, nullability));
    }

    /**
     * Create a system column for an identifer with consistent type of
     * VARCHAR(128)
     * 
     * @param name
     *            Name of the column.
     * @param nullability
     *            Nullability of the column.
     * @return Object representing the column.
     */
    static SystemColumn getIdentifierColumn(String name, boolean nullability) {
        return new SystemColumnImpl(name, DataTypeDescriptor
                .getBuiltInDataTypeDescriptor(Types.VARCHAR, nullability, 128));
    }

    /**
     * Create a system column for a character representation of a UUID with
     * consistent type of CHAR(36)
     * 
     * @param name
     *            Name of the column.
     * @param nullability
     *            Nullability of the column.
     * @return Object representing the column.
     */
    static SystemColumn getUUIDColumn(String name, boolean nullability) {
        return new SystemColumnImpl(name, DataTypeDescriptor
                .getBuiltInDataTypeDescriptor(Types.CHAR, nullability, 36));
    }

    /**
     * Create a system column for a character representation of an indicator
     * column with consistent type of CHAR(1) NOT NULL
     * 
     * @param name
     *            Name of the column.
     * @return Object representing the column.
     */
    static SystemColumn getIndicatorColumn(String name) {
        return new SystemColumnImpl(name, DataTypeDescriptor
                .getBuiltInDataTypeDescriptor(Types.CHAR, false, 1));
    }

    /**
     * Create a system column for a java column.
     * 
     * @param name
     *            Name of the column.
     * @param javaClassName
     * @param nullability
     *            Nullability of the column.
     * @return Object representing the column.
     */
    static SystemColumn getJavaColumn(String name, String javaClassName,
            boolean nullability) {

        TypeId typeId = TypeId.getUserDefinedTypeId(javaClassName, false);

        DataTypeDescriptor dtd = new DataTypeDescriptor(typeId, nullability);
        return new SystemColumnImpl(name, dtd);
    }

    /**
     * Create a SystemColumnImpl representing the given name and type.
     */
    private SystemColumnImpl(String name, DataTypeDescriptor type) {
        this.name = name;
        this.type = type;
    }

	/**
     * Constructor to create a description of a column in a system table.
     * 
     * @param name
     *            of column.
     * @param id
     *            of column.
     * @param nullability
     *            Whether or not column accepts nulls.
     * @param dataType
     *            Datatype of column.
     * @param maxLength
     *            Maximum length of data in column.
     */
	SystemColumnImpl(	String	name,
								int		id,
								boolean	nullability,
								String	dataType,
								boolean	builtInType,
								int		maxLength )
	{
		this.name			= name;
        
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
                               0,
                               0,
                               nullability,
                               maxLength
                               );
	}
    SystemColumnImpl(   String  name,
            int     id,
            int ignoreP,
            int ignoreS,
            boolean nullability,
            String  dataType,
            boolean builtInType,
            int     maxLength )
{
        this(name, id, nullability, dataType, builtInType, maxLength);
}

	/**
	 * Constructor to create a description of a column in a system table.
	 * This constructor is used for SQL Identifiers (varchar 128).
	 *
	 *	@param	name of column.
	 *	@param	id of column.
	 *	@param	nullability Whether or not column accepts nulls.
	 */
	SystemColumnImpl(	String	name,
								int		id,
								boolean	nullability)
	{
        this(name, id, nullability, "VARCHAR", true, 128);
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
     * Return the type of this column.
     */
    public DataTypeDescriptor getType() {
        return type;
    }
}

