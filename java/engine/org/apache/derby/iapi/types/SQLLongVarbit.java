/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.types
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.types;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.BitDataValue;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.Orderable;

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.types.NumberDataValue;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * SQLLongVarbit satisfies the DataValueDescriptor
 * interfaces (i.e., OrderableDataType). It implements a String holder,
 * e.g. for storing a column value; it can be specified
 * when constructed to not allow nulls. Nullability cannot be changed
 * after construction.
 * <p>
 * Because OrderableDataType is a subclass of DataType,
 * SQLLongVarbit can play a role in either a DataType/Value
 * or a OrderableDataType/KeyRow, interchangeably.
 *
 * It is an extension of SQLVarbit and is virtually indistinguishable
 * other than normalization.
 */
public class SQLLongVarbit extends SQLVarbit
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;

	public String getTypeName()
	{
		return TypeId.LONGVARBIT_NAME;
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLLongVarbit();
	}

	/** 
	 * @see DataValueDescriptor#setValueFromResultSet 
	 *
	 * @exception SQLException		Thrown on error
	 */
	public void setValueFromResultSet(ResultSet resultSet, int colNumber,
									  boolean isNullable)
		throws SQLException
	{
			stream = resultSet.getBinaryStream(colNumber);
			dataValue = null;
	}

	/**
		Return my format identifier.

		@see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId()
	{
		return StoredFormatIds.SQL_LONGVARBIT_ID;
	}


	/*
	 * Orderable interface
	 */


	/*
	 * Column interface
	 */


	/*
	 * class interface
	 */

	/*
	 * constructors
	 */
	public SQLLongVarbit()
	{
	}

	public SQLLongVarbit(byte[] val)
	{
		super(val);
	}

	/**
	 * Normalization method - this method may be called when putting
	 * a value into a SQLVarbit, for example, when inserting into a SQLVarbit
	 * column.  See NormalizeResultSet in execution.
	 *
	 * This overrides SQLBit -- the difference is that we don't
	 * expand SQLVarbits to fit the target.
	 * 
	 * @param desiredType	The type to normalize the source column to
	 * @param source		The value to normalize
	 *
	 *
	 * @exception StandardException				Thrown for null into
	 *											non-nullable column, and for
	 *											truncation error
	 */

	public void normalize(
				DataTypeDescriptor desiredType,
				DataValueDescriptor source)
					throws StandardException
	{
		if (source instanceof SQLLongVarbit) {
			// avoid creating an object in memory if a matching type.
			// this may be a stream.
			SQLLongVarbit other = (SQLLongVarbit) source;
			this.stream = other.stream;
			this.dataValue = other.dataValue;
		}
		else
			((SQLBinary) this).setValue(source.getBytes());
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.LONGVARBIT_PRECEDENCE;
	}
}
