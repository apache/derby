/*

   Derby - Class org.apache.derby.impl.store.raw.data.StoredFieldHeader

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
package org.apache.derby.impl.store.raw.data;

import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.services.sanity.SanityManager;

import java.io.IOException;
import java.io.EOFException;

import java.io.ObjectInput;
import java.io.OutputStream;

import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.derby.iapi.services.io.CompressedNumber;

import java.io.InputStream;

/**
    A class to provide static methods to manipulate fields in the field header.

	A class StoredPage uses to read/write field status and field data length.
	No attributes exist in this class, this class provides a set of static
	methods for writing field status and field data length, and for reading
	field status and field data length.

	<P><B>Stored Field Header Format</B><BR>
	The field header is broken into two sections.  
    Only the Status byte is required to be there.
	<PRE>
	Field header format:
	+--------+-------------------+
	| status | <fieldDataLength> |
	+--------+-------------------+
	Overflow page and overflow id are stored as field data.
	If the overflow bit in status is set, the field data is the overflow 
    information.  When the overflow bit is not set in status, then,
	fieldData is the actually user data for the field.
	That means, field header consists only field status, and field data length.

	A non-overflow field:
	+--------+-------------------+-------------+
	| status | <fieldDataLength> | <fieldData> |
	+--------+-------------------+-------------+

	An overflow field:
	+--------+-------------------+-----------------+--------------+
	| status | <fieldDataLength> | <overflow page> | <overflowID> |
	+--------+-------------------+-----------------+--------------+

	</PRE>
	<BR><B>status</B><BR>
	The status is 1 byte, it indicates the state of the field.

	A FieldHeader can be in the following states:
	NULL		- if the field is NULL, no field data length is stored
	OVERFLOW	- indicates the field has been overflowed to another page.
				  overflow page and overflow ID is stored at the end of the 
                  user data. field data length must be a number greater or 
                  equal to 0, indicating the length of the field that is stored
                  on the current page.

				  The format looks like this:
				  +--------+-----------------+---------------+------------+
				  |<status>|<fieldDataLength>|<overflow page>|<overflowID>|
				  +--------+-----------------+---------------+------------+

				  overflowPage will be written as compressed long,
				  overflowId will be written as compressed Int

	NONEXISTENT	- the field no longer exists, 
                  e.g. column has been dropped during an alter table

	EXTENSIBLE	- the field is of user defined data type.  
                  The field may be tagged.
                  
	TAGGED		- the field is TAGGED if and only if it is EXTENSIBLE.

	FIXED		- the field is FIXED if and only if it is used in the log 
                  records for version 1.2 and higher.

	<BR><B>fieldDataLength</B><BR>
	The fieldDataLength is only set if the field is not NULL.  It is the length
    of the field that is stored on the current page.
	The fieldDataLength is a variable length CompressedInt.
	<BR><B>overflowPage and overflowID</B><BR>
	The overflowPage is a variable length CompressedLong, overflowID is a 
    variable Length CompressedInt.
	They are only stored when the field state is OVERFLOW.
	And they are not stored in the field header.
	Instead, they are stored at the end of the field data.
	The reason we do that is to save a copy if the field has to overflow.

	<BR> MT - Mutable - Immutable identity - Thread Aware
*/
public final class StoredFieldHeader
{

    /**************************************************************************
     * Constants of the class
     **************************************************************************
     */

    // DO NOT use 0x80, some code reads byte into an int without masking the
    // sign bit, so do not use the high bit in the byte for a field status.
	private		static final int FIELD_INITIAL		= 0x00;
	public		static final int FIELD_NULL			= 0x01;
	public		static final int FIELD_OVERFLOW		= 0x02;
	private		static final int FIELD_NOT_NULLABLE	= 0x04;
	public		static final int FIELD_EXTENSIBLE	= 0x08;
	public		static final int FIELD_TAGGED		= 0x10;
	protected	static final int FIELD_FIXED		= 0x20;

	public		static final int FIELD_NONEXISTENT	= (FIELD_NOT_NULLABLE | FIELD_NULL);


    public static final int    STORED_FIELD_HEADER_STATUS_SIZE = 1;

    /**************************************************************************
     * Get accessors for testing bits in the status field.
     **************************************************************************
     */

	/**
		Get the status of the field

		<BR> MT - single thread required
	*/
	public static final boolean isNull(int status) {
		return ((status & FIELD_NULL) == FIELD_NULL);
	}

	public static final boolean isOverflow(int status) {
		return ((status & FIELD_OVERFLOW) == FIELD_OVERFLOW);
	}

	public static final boolean isNonexistent(int status) {
		return ((status & FIELD_NONEXISTENT) == FIELD_NONEXISTENT);
	}

	public static final boolean isExtensible(int status) {
		return ((status & FIELD_EXTENSIBLE) == FIELD_EXTENSIBLE);
	}

	public static final boolean isNullorNonExistent(int status) {
        // just need to check whether null bit is set.
        // return ((status & FIELD_NONEXISTENT) == FIELD_NONEXISTENT);
		return ((status & FIELD_NULL) != 0);
	}

	public static final boolean isTagged(int status) {
		//		if (SanityManager.DEBUG)
		//			SanityManager.ASSERT(isExtensible(status), "a field cannot be tagged if it is not extensible");
		return ((status & FIELD_TAGGED) == FIELD_TAGGED);
	}

	public static final boolean isFixed(int status) {
		return ((status & FIELD_FIXED) == FIELD_FIXED);
	}

	public static final boolean isNullable(int status) {
		return ((status & FIELD_NOT_NULLABLE) == 0);
	}

	public static final int size(
    int status, 
    int fieldDataLength, 
    int fieldDataSize) 
    {

        if ((status & (FIELD_NULL | FIELD_FIXED)) == 0)
        {
            // usual case - not-null, not-fixed

            // WARNING - the following code hand inlined from 
            // CompressedNumber for performance.
            //
            // return(CompressedNumber.sizeInt(fieldDataLength) + 1);
            //

            if (fieldDataLength <= 
                        CompressedNumber.MAX_COMPRESSED_INT_ONE_BYTE)
            {
                // compressed form is 1 byte
                return(2);
            }
            else if (fieldDataLength <= 
                        CompressedNumber.MAX_COMPRESSED_INT_TWO_BYTES)
            {
                // compressed form is 2 bytes
                return(3);
            }
            else
            {
                // compressed form is 4 bytes
                return(5);
            }
        }
        else if ((status & FIELD_NULL) != 0)
        {
            // field is null

			return(1);
		} 
        else
        {
            // fixed length field

            return((fieldDataSize > 2) ? 5 : 3);
		}
	}


    /**************************************************************************
     * Set accessors for setting bits in the status field.
     **************************************************************************
     */

	public final static int setInitial() {
		return FIELD_INITIAL;
	}

	public final static int setNull(int status, boolean isNull) {
		if (isNull)
			status |= FIELD_NULL;
		else
			status &= ~FIELD_NULL;
		return status;
	}

	public final static int setOverflow(int status, boolean isOverflow) {
		if (isOverflow)
			status |= FIELD_OVERFLOW;
		else
			status &= ~FIELD_OVERFLOW;
		return status;
	}

	public final static int setNonexistent(int status) {
		status |= FIELD_NONEXISTENT;
		return status;
	}

	public final static int setExtensible(int status, boolean isExtensible) {
		if (isExtensible)
			status |= FIELD_EXTENSIBLE;
		else
			status &= ~FIELD_EXTENSIBLE;
		return status;
	}

	public final static int setTagged(int status, boolean isTagged) {

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(isExtensible(status),
				"a field cannot be set to tagged if it is not extensible");

		if (isTagged)
			status |= FIELD_TAGGED;
		else
			status &= ~FIELD_TAGGED;
		return status;
	}

	public final static int setFixed(int status, boolean isFixed) {
		if (isFixed)
			status |= FIELD_FIXED;
		else
			status &= ~FIELD_FIXED;
		return status;
	}

    /**************************************************************************
     * routines used to write a field header to a OutputStream
     **************************************************************************
     */

	/**
		write out the field status and field data Length

		@exception IOException Thrown by potential I/O errors while writing 
                               field header.
	 */
	public static final int write(
    OutputStream out, 
    int status, 
    int fieldDataLength, 
    int fieldDataSize)
		throws IOException 
    {
		int len = 1;

		out.write(status);

		if (isNull(status))
			return len;

		if (isFixed(status)) 
        {
			// if the field header is for log, we write it in compressed format,
			// then we pad the field, so the total length is fixed.			
  			if (fieldDataSize > 2) 
            {
				int diffLen = 
                    fieldDataSize - 
                    CompressedNumber.writeInt(out, fieldDataLength);

				for (int i = diffLen; i > 0; i--)
					out.write(0);
				len += fieldDataSize;	// size of an int - 4 bytes
			} 
            else 
            {
				// write the int out as a short
				out.write((fieldDataLength >>> 8) & 0xFF);
				out.write((fieldDataLength >>> 0) & 0xFF);
				len += 2;	// size of a short - 2 bytes
			}

			// NOTE: fixed version is used for logs only,
			// the overflow information is stored at the end of the optional 
            // data, not in the field headers.  
            // That's why we are not writing overflow info here.

		} 
        else 
        {
			// if we are writing the fieldHeader for the page, 
            // we write in compressed format

			len += CompressedNumber.writeInt(out, fieldDataLength);
		}

		return len;
	}

    /**************************************************************************
     * routines used to read a field header from an ObjectInput stream, array
     **************************************************************************
     */

	/**
		read the field status

		@exception IOException Thrown by potential I/O errors while reading 
                               field header.
	 */
	public static final int readStatus(ObjectInput in) 
        throws IOException 
    {
		int status;

		if ((status = in.read()) >= 0)
            return status;
        else
			throw new EOFException();
	}

	public static final int readStatus(
    byte[]      page,
    int         offset)
    {
        return(page[offset]);
    }

    /**
     * read the length of the field and hdr
     * <p>
     * Optimized routine used to skip a field on a page.  It returns the
     * total length of the field including the header portion.  It operates
     * directly on the array and does no checking of it's own for limits on
     * the array length, so an array out of bounds exception may be thrown - 
     * the routine is meant to be used to read a field from a page so this
     * should not happen.
     * <p>
     *
	 * @return The length of the field on the page, including it's header.
     *
     * @param data      the array where the field is. 
     * @param offset    the offset in the array where the field begin, ie. 
     *                  the status byte is at data[offset].
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public static final int readTotalFieldLength(
    byte[]      data,
    int         offset)
			throws IOException 
    {
        if (SanityManager.DEBUG)
        {
            // this routine is meant to be called on the page, and FIXED fields
            // are only used in the log.

            if (isFixed(data[offset]))
                SanityManager.THROWASSERT("routine does not handle FIXED.");
        }

        if (((data[offset++]) & FIELD_NULL) != FIELD_NULL)
        {
            int value = data[offset];

            if ((value & ~0x3f) == 0)
            {
                // length is stored in this byte, we also know that the 0x80 bit
                // was not set, so no need to mask off the sign extension from
                // the byte to int conversion.

                // account for 1 byte stored length of field + 1 for status.
                return(value + 2);
            }
            else if ((value & 0x80) == 0)
            {
                // length stored in 2 bytes. only use low 6 bits from 1st byte.

                if (SanityManager.DEBUG)
                {
                    SanityManager.ASSERT((value & 0x40) == 0x40);
                }

                // top 8 bits of 2 byte length is stored in this byte, we also 
                // know that the 0x80 bit was not set, so no need to mask off 
                // the sign extension from the 1st byte to int conversion.  Need
                // to mask the byte in data[offset + 1] to account for possible
                // sign extension.

                // add 3 to account for 2 byte length + 1 for status

                return((((value & 0x3f) << 8) | (data[offset + 1] & 0xff)) + 3);
            }
            else
            {
                // length stored in 4 bytes.  only use low 7 bits from 1st byte.

                if (SanityManager.DEBUG)
                {
                    SanityManager.ASSERT((value & 0x80) == 0x80);
                }

                // top 8 bits of 4 byte length is stored in this byte, we also 
                // know that the 0x80 bit was set, so need to mask off the 
                // sign extension from the 1st byte to int conversion.  Need to
                // mask the bytes from the next 3 bytes data[offset + 1,2,3] to 
                // account for possible sign extension.


                // add 5 to account for 4 byte length + 1 added to all returns
                return(
                    (((value            & 0x7f) << 24) |
                     ((data[offset + 1] & 0xff) << 16) |
                     ((data[offset + 2] & 0xff) <<  8) |
                      (data[offset + 3] & 0xff)) + 5);
            }
        }
        else
        {
            return(1);
        }
	}


	public static final int readFieldLengthAndSetStreamPosition(
    byte[]              data,
    int                 offset,
    int                 status,
    int                 fieldDataSize,
    ArrayInputStream    ais)
			throws IOException 
    {
        if ((status & (FIELD_NULL | FIELD_FIXED)) == 0)
        {
            // usual case-not null, not fixed.  Length stored as compressed int.
            //   return(CompressedNumber.readInt(in));

            int value = data[offset++];

            if ((value & ~0x3f) == 0)
            {
                // usual case.

                // length is stored in this byte, we also know that the 0x80 bit
                // was not set, so no need to mask off the sign extension from
                // the byte to int conversion.

                // nothing to do, value already has int to return. 

            }
            else if ((value & 0x80) == 0)
            {
                // length is stored in 2 bytes.  use low 6 bits from 1st byte.

                if (SanityManager.DEBUG)
                {
                    SanityManager.ASSERT((value & 0x40) == 0x40);
                }

                // top 8 bits of 2 byte length is stored in this byte, we also 
                // know that the 0x80 bit was not set, so no need to mask off 
                // the sign extension from the 1st byte to int conversion.  
                // Need to mask the byte in data[offset + 1] to account for 
                // possible sign extension.

                value = (((value & 0x3f) << 8) | (data[offset++] & 0xff));

            }
            else
            {
                // length is stored in 4 bytes.  only low 7 bits from 1st byte.

                if (SanityManager.DEBUG)
                {
                    SanityManager.ASSERT((value & 0x80) == 0x80);
                }

                // top 8 bits of 4 byte length is stored in this byte, we also 
                // know that the 0x80 bit was set, so need to mask off the 
                // sign extension from the 1st byte to int conversion.  Need to
                // mask the bytes from the next 3 bytes data[offset + 1,2,3] to 
                // account for possible sign extension.


                // add 5 to account for 4 byte length + 1 added to all returns
                value = 
                    (((value          & 0x7f) << 24) |
                     ((data[offset++] & 0xff) << 16) |
                     ((data[offset++] & 0xff) <<  8) |
                      (data[offset++] & 0xff));
            }

            ais.setPosition(offset);

            return(value);
        }
        else if ((status & FIELD_NULL) != 0)
        {
            ais.setPosition(offset);
            return(0);
        }
        else
        {
            int fieldDataLength;

            // field data length is in a fixed size field, not compressed.

            if (fieldDataSize <= 2)
            {
                // read it in as short, because it was written out as short
                fieldDataLength = 
                    ((data[offset++] & 0xff) << 8) | (data[offset++] & 0xff);
            }
            else
            {
                // fieldDataLength = CompressedNumber.readInt(in);

                fieldDataLength = data[offset];

                if ((fieldDataLength & ~0x3f) == 0)
                {
                    // usual case.

                    // length is stored in this byte, we also know that the 0x80
                    // bit was not set, so no need to mask off the sign 
                    // extension from the byte to int conversion.

                    // nothing to do, fieldDataLength already has int to return.

                }
                else if ((fieldDataLength & 0x80) == 0)
                {
                    // len is stored in 2 bytes.  use low 6 bits from 1st byte.

                    if (SanityManager.DEBUG)
                    {
                        SanityManager.ASSERT((fieldDataLength & 0x40) == 0x40);
                    }

                    // top 8 bits of 2 byte length is stored in this byte, we 
                    // also know that the 0x80 bit was not set, so no need to 
                    // mask off the sign extension from the 1st byte to int 
                    // conversion.  Need to mask the byte in data[offset + 1] to
                    // account for possible sign extension.

                    fieldDataLength = 
                        (((fieldDataLength & 0x3f) << 8) | 
                         (data[offset + 1] & 0xff));

                }
                else
                {
                    // len is stored in 4 bytes.  only low 7 bits from 1st byte.

                    if (SanityManager.DEBUG)
                    {
                        SanityManager.ASSERT((fieldDataLength & 0x80) == 0x80);
                    }

                    // top 8 bits of 4 byte length is stored in this byte, we 
                    // also know that the 0x80 bit was set, so need to mask off
                    // the sign extension from the 1st byte to int conversion.  
                    // Need to mask the bytes from the next 3 bytes 
                    // data[offset + 1,2,3] to account for possible sign 
                    // extension.

                    fieldDataLength = 
                        (((fieldDataLength  & 0x7f) << 24) |
                         ((data[offset + 1] & 0xff) << 16) |
                         ((data[offset + 2] & 0xff) <<  8) |
                          (data[offset + 3] & 0xff));
                }

                offset = offset + fieldDataSize;
            } 

            ais.setPosition(offset);
            return(fieldDataLength);
        }


	}

	/**
		read the field data length

		@exception IOException Thrown by potential I/O errors while reading 
                               field header.
	 */
	public static final int readFieldDataLength(
    ObjectInput in, 
    int status, 
    int fieldDataSize)
			throws IOException 
    {
		
        if ((status & (FIELD_NULL | FIELD_FIXED)) == 0)
        {
            // usual case-not null, not fixed.  Length stored as compressed int.
            return(CompressedNumber.readInt(in));
        }
        else if ((status & FIELD_NULL) != 0)
        {
            // field is null or non-existent.
            return(0);
        }
        else
        {
            int fieldDataLength;

            // field data length is in a fixed size field, not compressed.

            if (fieldDataSize <= 2)
            {
                // read it in as short, because it was written out as short
                int ch1 = in.read();
                int ch2 = in.read();
                if ((ch1 | ch2) < 0)
                     throw new EOFException();

                fieldDataLength = ((ch1 << 8) + (ch2 << 0));
            }
            else
            {
                fieldDataLength = 
                    CompressedNumber.readInt(in);

                int diffLen = 
                    fieldDataSize - 
                    CompressedNumber.sizeInt(fieldDataLength);

                if (diffLen != 0)
                    in.skipBytes(diffLen);
            } 

            return(fieldDataLength);
        }
	}


	public static String toDebugString(int status)
	{
		if (SanityManager.DEBUG)
		{
			StringBuffer str = new StringBuffer(100);
			if (isNull(status)) str.append("Null ");
			if (isOverflow(status)) str.append("Overflow ");
			if (isNonexistent(status)) str.append("Nonexistent ");
			if (isExtensible(status)) str.append("Extensible ");
			if (isTagged(status)) str.append("Tagged ");
			if (isFixed(status)) str.append("Fixed ");
			if (isNullable(status)) str.append("Nullable ");
			if (str.length() == 0)
				str.append("INITIAL ");

			return str.toString();
		}
		return null;
	}
}
