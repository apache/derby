/*

   Derby - Class org.apache.derby.impl.sql.GenericColumnDescriptor

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.sql;

import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.Formatable;

import org.apache.derby.iapi.services.io.FormatableHashtable;
import org.apache.derby.iapi.services.io.FormatableIntHolder;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
/**
 * This is a stripped down implementation of a column
 * descriptor that is intended for generic use.  It
 * can be seralized and attached to plans.
 *
 * @author jamie
 */
public final class GenericColumnDescriptor
	implements ResultColumnDescriptor, Formatable
{

	/********************************************************
	**
	**	This class implements Formatable. That means that it
	**	can write itself to and from a formatted stream. If
	**	you add more fields to this class, make sure that you
	**	also write/read them with the writeExternal()/readExternal()
	**	methods.
	**
	**	If, inbetween releases, you add more fields to this class,
	**	then you should bump the version number emitted by the getTypeFormatId()
	**	method.
	**
	********************************************************/

	private String				name;
	private String				schemaName;
	private String				tableName;
	private int					columnPos;
	private DataTypeDescriptor	type;
	private boolean 			isAutoincrement;

	/**
	 * Niladic constructor for Formatable
	 */
	public GenericColumnDescriptor()
	{
	}

	public GenericColumnDescriptor(String name, DataTypeDescriptor	type) {
		this.name = name;
		this.type = type;
	}

	/** 
	 * This constructor is used to build a generic (and
	 * formatable) ColumnDescriptor.  The idea is that
	 * it can be passed a ColumnDescriptor from a query
	 * tree and convert it to something that can be used
	 * anywhere.
	 *
	 * @param rcd the ResultColumnDescriptor
	 */
	public GenericColumnDescriptor(ResultColumnDescriptor rcd)
	{
		name = rcd.getName();
		tableName = rcd.getSourceTableName();
		schemaName = rcd.getSchemaName();
		columnPos = rcd.getColumnPosition();
		type = rcd.getType();
		isAutoincrement = rcd.isAutoincrement();
	}

	/**
	 * Returns a DataTypeDescriptor for the column. This DataTypeDescriptor
	 * will not represent an actual value, it will only represent the type
	 * that all values in the column will have.
	 *
	 * @return	A DataTypeDescriptor describing the type of the column.
	 */
	public DataTypeDescriptor	getType()
	{
		return type;
	}

	/**
	 * Returns the name of the Column.
	 *
	 * @return	A String containing the name of the column.
	 */
	public String	getName()
	{
		return name;
	}

	/**
	 * Get the name of the schema the Column is in, if any.
	 *
	 * @return	A String containing the name of the schema the Column
	 *		is in.  If the column is not in a schema (i.e. is a
	 * 		derived column), it returns NULL.
	 */
	public String	getSchemaName()
	{
		return schemaName;
	}

	/**
	 * Get the name of the table the Column is in, if any.
	 *
	 * @return	A String containing the name of the table the Column
	 *		is in. If the column is not in a table (i.e. is a
	 * 		derived column), it returns NULL.
	 */
	public String	getSourceTableName()
	{
		return tableName;
	}

	/**
	 * Get the position of the Column.
	 * NOTE - position is 1-based.
	 *
	 * @return	An int containing the position of the Column
	 *		within the table.
	 */
	public int	getColumnPosition()
	{
		return columnPos;
	}

	public boolean isAutoincrement()
	{
		return isAutoincrement;
	}

	//////////////////////////////////////////////
	//
	// FORMATABLE
	//
	//////////////////////////////////////////////
	/**
	 * Write this object out
	 *
	 * @param out write bytes here
	 *
 	 * @exception IOException thrown on error
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		FormatableHashtable fh = new FormatableHashtable();
		fh.put("name", name);
		fh.put("tableName", tableName);
		fh.put("schemaName", schemaName);
		fh.putInt("columnPos", columnPos);
		fh.put("type", type);
		fh.putBoolean("isAutoincrement", isAutoincrement);
		out.writeObject(fh);
		return;
	}	

	public void djdrcd() {}
	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
	public void readExternal(ObjectInput in) 
		throws IOException, ClassNotFoundException
	{
		FormatableHashtable fh = (FormatableHashtable)in.readObject();		
		name = (String)fh.get("name");
		tableName = (String)fh.get("tableName");
		schemaName = (String)fh.get("schemaName");
		columnPos = fh.getInt("columnPos");
		type = (DataTypeDescriptor)fh.get("type");
		isAutoincrement = fh.getBoolean("isAutoincrement");
	}
	
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int getTypeFormatId()	{ return StoredFormatIds.GENERIC_COLUMN_DESCRIPTOR_V02_ID; }

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "GenericColumnDescriptor\n\tname: "+name+
				"\n\tTable: "+schemaName+"."+tableName+
				"\n\tcolumnPos: "+columnPos+
				"\n\tType: "+type;
		}
		else
		{
			return "";
		}
	}
}
