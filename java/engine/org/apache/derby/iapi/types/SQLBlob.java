/*

   Derby - Class org.apache.derby.iapi.types.SQLBlob

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.services.io.FormatableBitSet;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * SQLBlob satisfies the DataValueDescriptor,
 * interfaces (i.e., OrderableDataType). 
 * It uses the SQLLongVarbit implementation, which implements a String holder,
 * e.g. for storing a column value; it can be specified
 * when constructed to not allow nulls. Nullability cannot be changed
 * after construction.
 * <p>
 * Because LOB types are not orderable, we'll override those
 * methods...
 *
 */
public class SQLBlob extends SQLBinary
{

	/*
	 * constructors
	 */
	public SQLBlob()
        {
        }

	public SQLBlob(byte[] val)
        {
			super(val);
        }
	
	public String getTypeName()
        {
			return TypeId.BLOB_NAME;
        }

    /**
     * @see DataValueDescriptor#getNewNull
     */
	public DataValueDescriptor getNewNull()
        {
			return new SQLBlob();
        }

	/**
	 * Normalization method - this method may be called when putting
	 * a value into a SQLBit, for example, when inserting into a SQLBit
	 * column.  See NormalizeResultSet in execution.
	 *
	 * @param desiredType	The type to normalize the source column to
	 * @param source		The value to normalize
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
		setValue(source);
		setWidth(desiredType.getMaximumWidth(), 0, true);
	}

    // The method setWidth is only(?) used to adopt the value
    // to the casted domain/size. BLOBs behave different
    // from the BIT types in that a (CAST (X'01' TO BLOB(1024)))
    // does NOT pad the value to the maximal allowed datasize.
    // That it is done for BIT is understandable, however,
    // for BIT VARYING it is a bit confusing. Could be inheritence bug.
    // Anyhow, here we just ignore the call, since there is no padding to be done.
    // We do detect truncation, if the errorOnTrunc flag is set.
    // DB2 does return a WARNING on CAST and ERROR on INSERT.
	public DataValueDescriptor setWidth(int desiredWidth,  // ignored!
			int desiredScale,	// Ignored 
			boolean errorOnTrunc)
			throws StandardException
    {

		if (isNull())
			return this;

		int sourceWidth = getLength();

        // need to truncate?
        if (sourceWidth > desiredWidth) {
            if (errorOnTrunc)
                throw StandardException.newException(SQLState.LANG_STRING_TRUNCATION, getTypeName(), 
                                                     "XXXX",
                                                     String.valueOf(desiredWidth));
            else {
                /*
                 * Truncate to the desired width.
                 */
				

				byte[] shrunkData = new byte[desiredWidth];
				System.arraycopy(getBytes(), 0, shrunkData, 0, desiredWidth);
				dataValue = shrunkData;
            }
        }

        return this;
    }

    /**
	   Return my format identifier.
           
	   @see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId()
        {
			return StoredFormatIds.SQL_BLOB_ID;
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
			streamLength = -1; // unknown
			dataValue = null;
	}



	/*
	 * DataValueDescriptor interface
	 */
        
	/** @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
		{
			return TypeId.BLOB_PRECEDENCE; // not really used
		}

	}
