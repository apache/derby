/*

   Derby - Class org.apache.derby.iapi.types.SQLBlob

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

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.Limits;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.CloneableStream;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.sanity.SanityManager;

import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;

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
	
	public SQLBlob(Blob val)
        {
			super(val);
        }
	
	public String getTypeName()
        {
			return TypeId.BLOB_NAME;
        }

	/**
	 * Return max memory usage for a SQL Blob
	 */
	int getMaxMemoryUsage()
	{
		return Limits.DB2_LOB_MAXWIDTH;
	}

    /**
     * Tells if this BLOB value is, or will be, represented by a stream.
     *
     * @return {@code true} if the value is represented by a stream,
     *      {@code false} otherwise.
     */
    public boolean hasStream() {
        return stream != null;
    }

    /**
     * Returns a clone of this BLOB value.
     * <p>
     * Unlike the other binary types, BLOBs can be very large. We try to clone
     * the underlying stream when possible to avoid having to materialize the
     * value into memory.
     *
     * @param forceMaterialization any streams representing the data value will
     *      be materialized if {@code true}, the data value will be kept as a
     *      stream if possible if {@code false}
     * @return A clone of this BLOB value.
     */
    public DataValueDescriptor cloneValue(boolean forceMaterialization) {
        // TODO: Add optimization for materializing "smallish" streams. This
        //       may be more effective because the data doesn't have to be
        //       decoded multiple times.
        final SQLBlob clone = new SQLBlob();

        // Shortcut cases where value is NULL.
        if (isNull()) {
            return clone;
        }

        if (!forceMaterialization && dataValue == null) {
            if (stream != null && stream instanceof CloneableStream) {
                clone.setStream( ((CloneableStream)stream).cloneStream());
                if (streamValueLength != -1) {
                    clone.streamValueLength = streamValueLength;
                }
            } else if (_blobValue != null) {
                // Assumes the Blob object can be shared between value holders.
                clone.setValue(_blobValue);
            }
            // At this point we may still not have cloned the value because we
            // have a stream that isn't cloneable.
            // TODO: Add functionality to materialize to temporary disk storage
            //       to avoid OOME for large BLOBs.
        }

        // See if we are forced to materialize the value, either because
        // requested by the user or because we don't know how to clone it.
        if (clone.isNull() || forceMaterialization) {
            try {
                // NOTE: The byte array holding the value is shared.
                clone.setValue(getBytes());
            } catch (StandardException se) {
                if (SanityManager.DEBUG) {
                    SanityManager.THROWASSERT("Unexpected exception", se);
                }
                return null;
            }
        }
        return clone;
    }

    /**
     * @see DataValueDescriptor#getNewNull
     */
	public DataValueDescriptor getNewNull()
        {
			return new SQLBlob();
        }

     /**
      * Return a JDBC Blob. Originally implemented to support DERBY-2201.
      */
    public Object getObject()
        throws StandardException
    {
        // the generated code for the DERBY-2201 codepath expects to get a Blob
        // back.
        if ( _blobValue != null ) { return _blobValue; }
        else
        {
            byte[] bytes = getBytes();

            if ( bytes == null ) { return null; }
            else
            {
                try {
                    return new HarmonySerialBlob( bytes );
                } catch (SQLException se)
                {
                    throw StandardException.plainWrapException( se );
                }
            }
        }
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
	public void setWidth(int desiredWidth,  // ignored!
			int desiredScale,	// Ignored 
			boolean errorOnTrunc)
			throws StandardException
    {

		// Input is null, so there's nothing to do.
		if (isNull())
			return;

		// Input is a stream with unknown length. The length will be checked
		// while reading the stream.
		if (isLengthLess()) {
			return;
		}

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
	 * @throws StandardException 
	 */
	public void setValueFromResultSet(ResultSet resultSet, int colNumber,
									  boolean isNullable)
		throws SQLException, StandardException
	{
        Blob blob = resultSet.getBlob(colNumber);
        if (blob == null)
            setToNull();
        else
            setObject(blob);
	}



	/*
	 * DataValueDescriptor interface
	 */
        
	/** @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
		{
			return TypeId.BLOB_PRECEDENCE; // not really used
		}

    public void setInto(PreparedStatement ps, int position)
		throws SQLException, StandardException
	{
		if (isNull()) {
			ps.setBlob(position, (Blob)null);    
			return;
		}

		// This may cause problems for streaming blobs, by materializing the whole blob.
		ps.setBytes(position, getBytes());
    }
    
    /**
     * Set the value from an non-null object.
     */
    final void setObject(Object theValue)
        throws StandardException
    {
        Blob vb = (Blob) theValue;
        
        try {
            long vbl = vb.length();
            if (vbl < 0L || vbl > Integer.MAX_VALUE)
                throw this.outOfRange();
            
            setValue(new RawToBinaryFormatStream(
                    vb.getBinaryStream(), (int) vbl),
                    (int) vbl);
            
        } catch (SQLException e) {
            throw dataTypeConversion("DAN-438-tmp");
       }
    }

    /**
     * Tell if this blob is length less.
     *
     * @return <code>true</code> if the length of the blob is not known,
     *      <code>false</code> otherwise
     */
    private final boolean isLengthLess() {
        return (stream != null && streamValueLength < 0);
    }
}


