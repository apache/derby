/*

   Derby - Class org.apache.derby.iapi.types.SQLChar

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.StreamStorable;
import org.apache.derby.iapi.services.io.FormatIdInputStream;

import org.apache.derby.iapi.db.Database;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.ConcatableDataValue;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.error.StandardException;


import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.cache.ClassSize;
import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.iapi.services.i18n.LocaleFinder;

import org.apache.derby.iapi.db.DatabaseContext;

import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.SQLDate;
import org.apache.derby.iapi.types.SQLTime;
import org.apache.derby.iapi.types.SQLTimestamp;

import java.io.InputStream;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.io.EOFException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.CollationElementIterator;
import java.text.RuleBasedCollator;
import java.text.CollationKey;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Locale;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * SQLChar satisfies the DataValueDescriptor
 * interfaces (i.e., OrderableDataType). It implements an String holder,
 * e.g. for storing a column value; it can be specified
 * when constructed to not allow nulls. Nullability cannot be changed
 * after construction.
 * <p>
 * Because OrderableDataType is a subclass of DataType,
 * SQLChar can play a role in either a DataType/ValueRow
 * or a OrderableDataType/KeyRow, interchangeably.
 */
public class SQLChar
	extends DataType implements StringDataValue, StreamStorable
{

	/**
		Static array that can be used for blank padding.
	*/
	private static final char[] BLANKS = new char[40];
	static {
		for (int i = 0; i < BLANKS.length; i++) {
			BLANKS[i] = ' ';
		}
	}

	public static void appendBlanks(char[] ca, int offset, int howMany) {
		while (howMany > 0) {

			int count = howMany > BLANKS.length ? BLANKS.length : howMany;

			System.arraycopy(BLANKS, 0, ca, offset, count);
			howMany -= count;
			offset += count;
		}
	}
	/*
	 * DataValueDescriptor interface
	 * (mostly implemented in DataType)
	 * casts to the
	 * numeric and date/time types as well, "for valid strings"
	 */

	/**
	 * @see DataValueDescriptor#getBoolean
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean getBoolean()
		throws StandardException
	{
		if (isNull()) return false;

		// match JCC, match only "0" or "false" for false. No case insensitivity.
		// everything else is true.

		String cleanedValue = getString().trim();

		return !(cleanedValue.equals("0") || cleanedValue.equals("false"));
	}

	/**
	 * @see DataValueDescriptor#getByte
	 * @exception StandardException thrown on failure to convert
	 */
	public byte	getByte() throws StandardException
	{
		if (isNull()) return (byte)0;
		try {
			return Byte.parseByte(getString().trim());
		} catch (NumberFormatException nfe) {
			throw StandardException.newException(SQLState.LANG_FORMAT_EXCEPTION, "byte");
		}
	}

	/**
	 * @see DataValueDescriptor#getShort
	 * @exception StandardException thrown on failure to convert
	 */
	public short	getShort() throws StandardException
	{
		if (isNull()) return (short)0;
		try {
			return Short.parseShort(getString().trim());
		} catch (NumberFormatException nfe) {
			throw StandardException.newException(SQLState.LANG_FORMAT_EXCEPTION, "short");
		}
	}

	/**
	 * @see DataValueDescriptor#getInt
	 * @exception StandardException thrown on failure to convert
	 */
	public int	getInt() throws StandardException
	{
		if (isNull()) return 0;
		try {
			return Integer.parseInt(getString().trim());
		} catch (NumberFormatException nfe) {
			throw StandardException.newException(SQLState.LANG_FORMAT_EXCEPTION, "int");
		}
	}

	/**
	 * @see DataValueDescriptor#getLong
	 * @exception StandardException thrown on failure to convert
	 */
	public long	getLong() throws StandardException
	{
		if (isNull()) return 0;
		try {
			return Long.parseLong(getString().trim());
		} catch (NumberFormatException nfe) {
			throw StandardException.newException(SQLState.LANG_FORMAT_EXCEPTION, "long");
		}
	}

	/**
	 * @see DataValueDescriptor#getFloat
	 * @exception StandardException thrown on failure to convert
	 */
	public float	getFloat() throws StandardException
	{
		if (isNull()) return 0;
		try {
			return new Float(getString().trim()).floatValue();
		} catch (NumberFormatException nfe) {
			throw StandardException.newException(SQLState.LANG_FORMAT_EXCEPTION, "float");
		}
	}

	/**
	 * @see DataValueDescriptor#getDouble
	 * @exception StandardException thrown on failure to convert
	 */
	public double	getDouble() throws StandardException
	{
		if (isNull()) return 0;
		try {
			return new Double(getString().trim()).doubleValue();
		} catch (NumberFormatException nfe) {
			throw StandardException.newException(SQLState.LANG_FORMAT_EXCEPTION, "double");
		}
	}

	/**
	 * @see DataValueDescriptor#getBigDecimal
	 * @exception StandardException thrown on failure to convert
	 */
	public BigDecimal	getBigDecimal() throws StandardException
	{
		if (isNull()) return null;
		try {
		    return new BigDecimal(getString().trim());
		} catch (NumberFormatException nfe) {
			throw StandardException.newException(SQLState.LANG_FORMAT_EXCEPTION, "java.math.BigDecimal");
		}
	}

	/**
	 * @see DataValueDescriptor#getDate
	 * @exception StandardException thrown on failure to convert
	 */
	public Date	getDate( Calendar cal) throws StandardException
	{
        return getDate( cal, getString(), getLocaleFinder());
	}

    public static Date getDate(java.util.Calendar cal, String str, LocaleFinder localeFinder) throws StandardException
    {
        if( str == null)
            return null;
        SQLDate internalDate = new SQLDate( str, false, localeFinder);
        return internalDate.getDate( cal);
    }

	/**
	 * @see DataValueDescriptor#getTime
	 * @exception StandardException thrown on failure to convert
	 */
	public Time	getTime(Calendar cal) throws StandardException
	{
		return getTime( cal, getString(), getLocaleFinder());
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public static Time getTime( Calendar cal, String str, LocaleFinder localeFinder) throws StandardException
	{
        if( str == null)
            return null;
        SQLTime internalTime = new SQLTime( str, false, localeFinder, cal);
        return internalTime.getTime( cal);
	}

	/**
	 * @see DataValueDescriptor#getTimestamp
	 * @exception StandardException thrown on failure to convert
	 */
	public Timestamp getTimestamp( Calendar cal) throws StandardException
	{
		return getTimestamp( cal, getString(), getLocaleFinder());
	}

	/**
	 * @see DataValueDescriptor#getTimestamp
	 * @exception StandardException thrown on failure to convert
	 */
	public static Timestamp	getTimestamp(java.util.Calendar cal, String str, LocaleFinder localeFinder)
        throws StandardException
	{
        if( str == null)
            return null;
        SQLTimestamp internalTimestamp = new SQLTimestamp( str, false, localeFinder, cal);
        return internalTimestamp.getTimestamp( cal);
	}

	/**
	 * @exception StandardException		Thrown on error
	 */
	public Object	getObject() throws StandardException
	{
		return getString();
	}

	/**
	 * @exception StandardException		Thrown on error
	 */
	public InputStream	getStream() throws StandardException
	{
		return stream;
	}

	/**
	 * @exception StandardException		Thrown on error
	 */
	public int	getLength() throws StandardException
	{
		if (rawLength != -1)
			return rawLength;

		String tmpString = getString();
		return (tmpString == null) ?
			0 : tmpString.length();
	}

	public String getTypeName()
	{
		return TypeId.CHAR_NAME;
	}

	/**
	 * If possible, use getCharArray() if you don't really
	 * need a string.  getString() will cause an extra 
	 * char array to be allocated when it calls the the String() 
	 * constructor (the first time through), so may be
	 * cheaper to use getCharArray().
	 *
	 * @exception StandardException		Thrown on error
	 */
	public String getString() throws StandardException
	{
		if (value == null) {

			int len = rawLength;

			if (len != -1) {

				// data is stored in the char[] array

				value = new String(rawData, 0, len);
				if (len > 4096) {
					// free up this char[] array to reduce memory usage
					rawData = null;
					rawLength = -1;
					// clear out the int array as well, so it will stay current
					intArray = null;
					intLength = 0;
					cKey = null;
				}

			} else if (stream != null) {

				// data stored as a stream
				try {

					if (stream instanceof FormatIdInputStream) {
						readExternal((FormatIdInputStream) stream);
					} else {
						readExternal(new FormatIdInputStream(stream));
					}
					stream = null;

					// at this point the value is only in the char[]
					// so call again to convert to a String
					return getString();

				} catch (IOException ioe) {

					throw StandardException.newException(
                            SQLState.LANG_STREAMING_COLUMN_I_O_EXCEPTION, 
                            ioe, 
                            "java.sql.String");
				}
			}
		}

		return value;
	}

	/**
	 * Get a char array.  Typically, this is a simple
	 * getter that is cheaper than getString() because
	 * we always need to create a char array when
	 * doing I/O.  Use this instead of getString() where
	 * reasonable.
	 * <p>
	 * <b>WARNING</b>: may return a character array that has spare
	 * characters at the end.  MUST be used in conjunction
	 * with getLength() to be safe.
	 * 
	 * @exception StandardException		Thrown on error
	 */
	public char[] getCharArray() throws StandardException
	{
		if (isNull())
		{
			return (char[])null;
		}
		else if (rawLength != -1)
		{
			return rawData;
		}
		else
		{
			// this is expensive -- we are getting a
			// copy of the char array that the 
			// String wrapper uses.
			getString();
			rawData = value.toCharArray();
			rawLength = rawData.length;
			// clear out the int array as well, so it will stay current
			intArray = null;
			intLength = 0;
			cKey = null;
			return rawData;
		}
	}

	/*
	 * StreamStorable interface : 
	 */
	public InputStream returnStream()
	{
		return stream;
	}

	public void setStream(InputStream newStream)
	{
		this.value = null;
		this.rawLength = -1;
		this.stream = newStream;
		// clear out the int array as well, so it will stay current
		intArray = null;
		intLength = 0;
		cKey = null;
	}

	public void loadStream() throws StandardException
	{
		getString();
	}

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */

	/**
		Return my format identifier.

		@see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.SQL_CHAR_ID;
	}

	/**
	 * see if the String value is null.
	 @see Storable#isNull
	*/
	public boolean isNull()
	{
		return ((value == null) && (rawLength == -1) && (stream == null));
	}

	/**
		The maximum stored size is based upon the UTF format
		used to stored the String. The format consists of
		a two byte length field and a maximum number of three
		bytes for each character.
		<BR>
		This puts an upper limit on the length of a stored
		String. The maximum stored length is 65535, these leads to
		the worse case of a maximum string length of 21844 ((65535 - 2) / 3).
		<BR>
		Strings with stored length longer than 64K is handled with
		the following format:
		(1) 2 byte length: will be assigned 0.
		(2) UTF formated string data.
		(3) terminate the string with the following 3 bytes:
			first byte is:
			+---+---+---+---+---+---+---+---+
			| 1 | 1 | 1 | 0 | 0 | 0 | 0 | 0 |
			+---+---+---+---+---+---+---+---+
			second byte is:
			+---+---+---+---+---+---+---+---+
			| 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
			+---+---+---+---+---+---+---+---+
			third byte is:
			+---+---+---+---+---+---+---+---+
			| 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
			+---+---+---+---+---+---+---+---+


		The UTF format:
		Writes a string to the underlying output stream using UTF-8 
		encoding in a machine-independent manner. 
		<p>
		First, two bytes are written to the output stream as if by the 
		<code>writeShort</code> method giving the number of bytes to 
		follow. This value is the number of bytes actually written out, 
		not the length of the string. Following the length, each character 
		of the string is output, in sequence, using the UTF-8 encoding 
		for the character. 
		@param      str   a string to be written.
		@exception  IOException  if an I/O error occurs.
		@since      JDK1.0


	  @exception IOException thrown by writeUTF

	  @see java.io.DataInputStream

	*/
	public void writeExternal(ObjectOutput out) throws IOException
	{
		// never called when value is null
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isNull());

		String lvalue = null;
		char[] data = null;

		int strlen = rawLength;
		boolean isRaw;

		if (strlen < 0) {
			lvalue = value;
			strlen = lvalue.length();
			isRaw = false;
		} else {
			data = rawData;
			isRaw = true;
		}

		// byte length will always be at least string length
		int utflen = strlen;

		for (int i = 0 ; (i < strlen) && (utflen <= 65535); i++)
		{
			int c = isRaw ? data[i] : lvalue.charAt(i);
			if ((c >= 0x0001) && (c <= 0x007F))
			{
				// 1 byte for character
			}
			else if (c > 0x07FF)
			{
				utflen += 2; // 3 bytes for character
			}
			else
			{
				utflen += 1; // 2 bytes for character
			}
		}

		boolean isLongUTF = false;
		// for length than 64K, see format description above
		if (utflen > 65535)
		{
			isLongUTF = true;
			utflen = 0;
		}

		out.write((utflen >>> 8) & 0xFF);
		out.write((utflen >>> 0) & 0xFF);
		for (int i = 0 ; i < strlen ; i++)
		{
			int c = isRaw ? data[i] : lvalue.charAt(i);
			if ((c >= 0x0001) && (c <= 0x007F))
			{
				out.write(c);
			}
			else if (c > 0x07FF)
			{
				out.write(0xE0 | ((c >> 12) & 0x0F));
				out.write(0x80 | ((c >>  6) & 0x3F));
				out.write(0x80 | ((c >>  0) & 0x3F));
			}
			else
			{
				out.write(0xC0 | ((c >>  6) & 0x1F));
				out.write(0x80 | ((c >>  0) & 0x3F));
			}
		}

		if (isLongUTF)
		{
			// write the following 3 bytes to terminate the string:
			// (11100000, 00000000, 00000000)
			out.write(0xE0);
			out.write(0);
			out.write(0);
		}
	}

    /**
     * Reads in a string from the specified data input stream. The 
     * string has been encoded using a modified UTF-8 format. 
     * <p>
     * The first two bytes are read as if by 
     * <code>readUnsignedShort</code>. This value gives the number of 
     * following bytes that are in the encoded string, not
     * the length of the resulting string. The following bytes are then 
     * interpreted as bytes encoding characters in the UTF-8 format 
     * and are converted into characters. 
     * <p>
     * This method blocks until all the bytes are read, the end of the 
     * stream is detected, or an exception is thrown. 
     *
     * @param      in   a data input stream.
     * @return     a Unicode string.
     * @exception  EOFException            if the input stream reaches the end
     *               before all the bytes.
     * @exception  IOException             if an I/O error occurs.
     * @exception  UTFDataFormatException  if the bytes do not represent a
     *               valid UTF-8 encoding of a Unicode string.
     * @see        java.io.DataInputStream#readUnsignedShort()
	 
	 * @see java.io.Externalizable#readExternal
     */
	public void readExternalFromArray(ArrayInputStream in) 
        throws IOException
    {
        arg_passer[0]        = rawData;

        rawLength = in.readCloudscapeUTF(arg_passer);

        rawData = arg_passer[0];

        // restoreToNull();
        value  = null;
        stream = null;

        // clear out the int array, so it will stay current
        intArray = null;
        intLength = 0;
        cKey = null;
    }
    char[][] arg_passer = new char[1][];

	public void readExternal(ObjectInput in) throws IOException
    {
        // if in.available() blocked at 0, use this default string size 

        int utflen = in.readUnsignedShort();

        int requiredLength;
        if (utflen != 0)
        {
            // the object was not stored as a streaming column 
            // we know exactly how long it is
            requiredLength = utflen;
        }
        else
        {
            // the object was stored as a streaming column 
            // and we have a clue how much we can read unblocked 
            // OR
            // The original string was a 0 length string.
            requiredLength = in.available();
            if (requiredLength < 64)
                requiredLength = 64;
        }

        char str[];
        if ((rawData == null) || (requiredLength > rawData.length)) {
            
            str = new char[requiredLength];
        } else {
            str = rawData;
        }
        int arrayLength = str.length;

        // Set these to null to allow GC of the array if required.
        rawData = null;
        restoreToNull();

        int count = 0;
        int strlen = 0;

readingLoop:
        while ( ((count < utflen) || (utflen == 0)))
        {
            int c;

            try {

                c = in.readUnsignedByte();
            } catch (EOFException eof) {
                if (utflen != 0)
                    throw new EOFException();

                // This is the case for a 0 length string.
                // OR the string was originally streamed in
                // which puts a 0 for utflen but no trailing
                // E0,0,0 markers.
                break readingLoop;
            }

            //if (c == -1)		// read EOF
            //{
            //	if (utflen != 0)
            //		throw new EOFException();

            //	break;
            //}

            // change it to an unsigned byte
            //c &= 0xFF;

            if (strlen >= arrayLength) // the char array needs to be grown 
            {
                int growby = in.available();

                // We know at the array needs to be grown by at least one.
                // However, even if the input stream wants to block on every
                // byte, we don't want to grow by a byte at a time.
                if (growby < 64)
                    growby = 64;

                int newstrlength = arrayLength + growby;
                char oldstr[] = str;
                str = new char[newstrlength];

                System.arraycopy(oldstr, 0, str, 0, arrayLength);
                arrayLength = newstrlength;
            }

            /// top fours bits of the first unsigned byte that maps to a 
            //  1,2 or 3 byte character
            //
            // 0000xxxx	- 0 - 1 byte char
            // 0001xxxx - 1 - 1 byte char
            // 0010xxxx - 2 - 1 byte char
            // 0011xxxx - 3 - 1 byte char
            // 0100xxxx - 4 - 1 byte char
            // 0101xxxx - 5 - 1 byte char
            // 0110xxxx - 6 - 1 byte char
            // 0111xxxx - 7 - 1 byte char
            // 1000xxxx - 8 - error
            // 1001xxxx - 9 - error
            // 1010xxxx - 10 - error
            // 1011xxxx - 11 - error
            // 1100xxxx - 12 - 2 byte char
            // 1101xxxx - 13 - 2 byte char
            // 1110xxxx - 14 - 3 byte char
            // 1111xxxx - 15 - error

            int char2, char3;
            char actualChar;
            if ((c & 0x80) == 0x00)
            {
                // one byte character
                count++;
                actualChar = (char) c;
            }
            else if ((c & 0x60) == 0x40) // we know the top bit is set here
            { 
                // two byte character
                count += 2;
                if (utflen != 0 && count > utflen) 
                    throw new UTFDataFormatException();		  
                char2 = in.readUnsignedByte();
                if ((char2 & 0xC0) != 0x80)
                    throw new UTFDataFormatException();		  
                actualChar = (char)(((c & 0x1F) << 6) | (char2 & 0x3F));
            }
            else if ((c & 0x70) == 0x60) // we know the top bit is set here
            {
                // three byte character
                count += 3;
                if (utflen != 0 && count > utflen) 
                    throw new UTFDataFormatException();		  
                char2 = in.readUnsignedByte();
                char3 = in.readUnsignedByte();
                if ((c == 0xE0) && (char2 == 0) && (char3 == 0)
                    && (utflen == 0))
                {
                    // we reached the end of a long string,
                    // that was terminated with
                    // (11100000, 00000000, 00000000)
                    break readingLoop;
                }

                if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
                    throw new UTFDataFormatException();		  
                
                
                actualChar = (char)(((c & 0x0F) << 12) |
                                           ((char2 & 0x3F) << 6) |
                                           ((char3 & 0x3F) << 0));
            }
            else {

                throw new UTFDataFormatException();
            }

            str[strlen++] = actualChar;
        }


        rawData = str;
        rawLength = strlen;
                        
        // clear out the int array, so it will stay current
        intArray = null;
        intLength = 0;
        cKey = null;
    }

	/**
	 * @see Storable#restoreToNull
	 *
	 */
	public void restoreToNull()
	{
		value = null;
		stream = null;
		rawLength = -1;
		// clear out the int array as well, so it will stay current
		intArray = null;
		intLength = 0;
		cKey = null;
	}

	/**
		@exception StandardException thrown on error
	 */
	public boolean compare(int op,
						   DataValueDescriptor other,
						   boolean orderedNulls,
						   boolean unknownRV)
		throws StandardException
	{
		if (!orderedNulls)		// nulls are unordered
		{
			if (this.isNull() || ((DataValueDescriptor) other).isNull())
				return unknownRV;
		}

		/* When comparing String types to non-string types, we always
		 * convert the string type to the non-string type.
		 */
		if (! (other instanceof SQLChar))
		{
			return other.compare(flip(op), this, orderedNulls, unknownRV);
		}

		/* Do the comparison */
		return super.compare(op, other, orderedNulls, unknownRV);
	}

	/**
		@exception StandardException thrown on error
	 */
	public int compare(DataValueDescriptor other) throws StandardException
	{
		/* Use compare method from dominant type, negating result
		 * to reflect flipping of sides.
		 */
		if (typePrecedence() < other.typePrecedence())
		{
			return - (other.compare(this));
		}

		// stringCompare deals with null as comparable and smallest
		return stringCompare(this, (SQLChar)other);
	}

	/*
	 * CloneableObject interface
	 */

	/** From CloneableObject
	 *	Shallow clone a StreamStorable without objectifying.  This is used to avoid
	 *	unnecessary objectifying of a stream object.  The only difference of this method
	 *  from getClone is this method does not objectify a stream.  beetle 4896
	 */
	public Object cloneObject()
	{
		if (stream == null)
			return getClone();
		SQLChar self = (SQLChar) getNewNull();
		self.copyState(this);
		return self;
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#getClone */
	public DataValueDescriptor getClone()
	{
		try
		{
			return new SQLChar(getString());
		}
		catch (StandardException se)
		{
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("Unexpected exception " + se);
			return null;
		}
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 *
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLChar();
	}

	/** 
	 * @see DataValueDescriptor#setValueFromResultSet 
	 *
	 * @exception SQLException		Thrown on error
	 */
	public final void setValueFromResultSet(ResultSet resultSet, int colNumber,
									  boolean isNullable)
		throws SQLException
	{
			setValue(resultSet.getString(colNumber));
	}

	/**
		Set the value into a PreparedStatement.
	*/
	public final void setInto(PreparedStatement ps, int position) throws SQLException, StandardException {

		ps.setString(position, getString());
	}


	/*
	 * class interface
	 */

	/*
	 * constructors
	 */

	/**
		no-arg constructor, required by Formattable.
	*/
	public SQLChar()
	{
	}

	public SQLChar(String val)
	{
		value = val;
	}

	public void setValue(String theValue)
	{
		stream = null;
		rawLength = -1;
		// clear out the int array as well, so it will stay current
		intArray = null;
		intLength = 0;
		cKey = null;

		value = theValue;
	}

	public void setValue(boolean theValue) throws StandardException
	{
		// match JCC.
		setValue(theValue ? "1" : "0");
	}

	/**
	 * Set the value of this SQLChar to the given boolean.
	 *
	 * @param theValue	The Boolean
	 *
	 * @return	This as Boolean
	 *
	 */
	public void setValue(Boolean theValue)  throws StandardException
	{
		setValue((Object) theValue);
	}

	public void setValue(int theValue)  throws StandardException
	{
		setValue(Integer.toString(theValue));
	}

	public void setValue(Integer theValue)  throws StandardException
	{
		setValue((Object) theValue);
	}

	public void setValue(double theValue)  throws StandardException
	{
		setValue(Double.toString(theValue));
	}

	public void setValue(Double theValue)  throws StandardException
	{
		setValue((Object) theValue);
	}

	public void setValue(float theValue)  throws StandardException
	{
		setValue(Float.toString(theValue));
	}

	public void setValue(Float theValue) throws StandardException
	{
		setValue((Object) theValue);
	}

	public void setValue(short theValue)  throws StandardException
	{
		setValue(Short.toString(theValue));
	}

	public void setValue(Short theValue) throws StandardException
	{
		setValue((Object) theValue);
	}

	public void setValue(long theValue)  throws StandardException
	{
		setValue(Long.toString(theValue));
	}

	public void setValue(Long theValue) throws StandardException
	{
		setValue((Object) theValue);
	}

	public void setValue(byte theValue)  throws StandardException
	{
		setValue(Byte.toString(theValue));
	}

	public void setValue(Byte theValue) throws StandardException
	{
		setValue((Object) theValue);
	}

	public void setValue(byte[] theValue) throws StandardException
	{
		if (theValue == null)
		{
			restoreToNull();
			return;
		}

		/*
		** We can't just do a new String(theValue)
		** because that method assumes we are converting
		** ASCII and it will take on char per byte.
		** So we need to convert the byte array to a
		** char array and go from there.
		**
		** If we have an odd number of bytes pad out.
		*/
		int mod = (theValue.length % 2);
		int len = (theValue.length/2) + mod;
		char[] carray = new char[len];
		int cindex = 0;
		int bindex = 0;

		/*
		** If we have a left over byte, then get
		** that now.
		*/
		if (mod == 1)
		{
			carray[--len] = (char)(theValue[theValue.length - 1] << 8);
		}

		for (; cindex < len; bindex+=2, cindex++)
		{
			carray[cindex] = (char)((theValue[bindex] << 8) |
								(theValue[bindex+1] & 0x00ff));
		}

		setValue(new String(carray));
	}

	public void setValue(BigDecimal theValue)  throws StandardException
	{
		setValue((Object) theValue);
	}

	/** @exception StandardException		Thrown on error */
	public void setValue(Date theValue, Calendar cal) throws StandardException
	{
        String strValue = null;
        if( theValue != null)
        {
            if( cal == null)
                strValue = theValue.toString();
            else
            {
                cal.setTime( theValue);
                StringBuffer sb = new StringBuffer();
                formatJDBCDate( cal, sb);
                strValue= sb.toString();
            }
        }
        setValue( strValue);
	}

	/** @exception StandardException		Thrown on error */
	public void setValue(Time theValue, Calendar cal) throws StandardException
	{
        String strValue = null;
        if( theValue != null)
        {
            if( cal == null)
                strValue = theValue.toString();
            else
            {
                cal.setTime( theValue);
                StringBuffer sb = new StringBuffer();
                formatJDBCTime( cal, sb);
                strValue= sb.toString();
            }
        }
        setValue( strValue);
	}

	/** @exception StandardException		Thrown on error */
	public void setValue(Timestamp theValue, Calendar cal) throws StandardException
	{
        String strValue = null;
        if( theValue != null)
        {
            if( cal == null)
                strValue = theValue.toString();
            else
            {
                cal.setTime( theValue);
                StringBuffer sb = new StringBuffer();
                formatJDBCDate( cal, sb);
                sb.append( ' ');
                formatJDBCTime( cal, sb);
                int micros = (theValue.getNanos() + SQLTimestamp.FRACTION_TO_NANO/2)/SQLTimestamp.FRACTION_TO_NANO;
                if( micros > 0)
                {
                    sb.append( '.');
                    String microsStr = Integer.toString( micros);
                    if( microsStr.length() > SQLTimestamp.MAX_FRACTION_DIGITS)
                        sb.append( microsStr.substring( 0, SQLTimestamp.MAX_FRACTION_DIGITS));
                    else
                    {
                        for( int i = microsStr.length(); i < SQLTimestamp.MAX_FRACTION_DIGITS ; i++)
                            sb.append( '0');
                        sb.append( microsStr);
                    }
                }
                strValue= sb.toString();
            }
        }
        setValue( strValue);
	}

    private void formatJDBCDate( Calendar cal, StringBuffer sb)
    {
        SQLDate.dateToString( cal.get( Calendar.YEAR),
                              cal.get( Calendar.MONTH) - Calendar.JANUARY + 1,
                              cal.get( Calendar.DAY_OF_MONTH),
                              sb);
    }

    private void formatJDBCTime( Calendar cal, StringBuffer sb)
    {
        SQLTime.timeToString( cal.get( Calendar.HOUR), cal.get( Calendar.MINUTE), cal.get( Calendar.SECOND), sb);
    }

	/**
	 * @see SQLChar#setValue
	 *
	 */
	public void setValue(InputStream theStream, int streamLength)
	{
		value = null;
		rawLength = -1;
		stream = theStream;
		// clear out the int array as well, so it will stay current
		intArray = null;
		intLength = 0;
		cKey = null;
	}

	/**
	 * @see DataValueDescriptor#setValue
	 */
	public void setValue(Object theValue)  throws StandardException
	{
		if ((theValue instanceof String) ||
			(theValue == null))
		{
			setValue((String) theValue);
		}
		else
		{

			{
				setValue(theValue.toString());
			}
		}
	}
	protected void setFrom(DataValueDescriptor theValue) throws StandardException {

		setValue(theValue.getString());
	}


	private void setAsToNationalString(Object theValue) 
	{
		String s = null;
		if (theValue != null)
			s = theValue.toString();
		setValue(s);
	}

	/**
	 * Normalization method - this method may be called when putting
	 * a value into a SQLChar, for example, when inserting into a SQLChar
	 * column.  See NormalizeResultSet in execution.
	 *
	 * @param desiredType	The type to normalize the source column to
	 * @param source		The value to normalize
	 * @param cachedDest	DataValueDescriptor, if non-null, to hold result
	 *						(Reuse if normalizing multiple rows)
	 *
	 * @return	The normalized SQLChar
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

		normalize(desiredType, source.getString());

	}

	protected void normalize(DataTypeDescriptor desiredType, String sourceValue)
		throws StandardException
	{


		int	desiredWidth = desiredType.getMaximumWidth();
		int sourceWidth = sourceValue.length();

		/*
		** If the input is already the right length, no normalization is
		** necessary - just return the source.
		*/
		if (sourceWidth == desiredWidth) {
			setValue(sourceValue);
			return;
		}

		/*
		** If the input is shorter than the desired type, construct a new
		** SQLChar padded with blanks to the right length.
		*/
		if (sourceWidth < desiredWidth)
		{
			setToNull();

			char[] ca;
			if ((rawData == null) || (desiredWidth > rawData.length)) {
			
				ca = rawData = new char[desiredWidth];
			} else {
				ca = rawData;
			}

			sourceValue.getChars(0, sourceWidth, ca, 0);
			SQLChar.appendBlanks(ca, sourceWidth, desiredWidth - sourceWidth);

			rawLength = desiredWidth;

			return;
		}

		/*
		** Check whether any non-blank characters will be truncated.
		*/
		hasNonBlankChars(sourceValue, desiredWidth, sourceWidth);

		/*
		** No non-blank characters will be truncated.  Truncate the blanks
		** to the desired width.
		*/

		String truncatedString = sourceValue.substring(0, desiredWidth);
		setValue(truncatedString);
	}

	/*
	** Method to check for truncation of non blank chars.
	*/
	protected final void hasNonBlankChars(String source, int start, int end)
		throws StandardException
	{
		/*
		** Check whether any non-blank characters will be truncated.
		*/
		for (int posn = start; posn < end; posn++)
		{
			if (source.charAt(posn) != ' ')
			{
				throw StandardException.newException(SQLState.LANG_STRING_TRUNCATION, getTypeName(), StringUtil.formatForPrint(source), String.valueOf(start));
			}
		}
	}

	///////////////////////////////////////////////////////////////
	//
	// VariableSizeDataValue INTERFACE
	//
	///////////////////////////////////////////////////////////////
	
	/**
	 * Set the width of the to the desired value.  Used
	 * when CASTing.  Ideally we'd recycle normalize(), but
	 * the behavior is different (we issue a warning instead
	 * of an error, and we aren't interested in nullability).
	 *
	 * @param desiredWidth	the desired length
	 * @param desiredScale	the desired scale (ignored)
	 * @param errorOnTrunc	throw an error on truncation
	 * @return this with the new width
	 *
	 * @exception StandardException		Thrown when errorOnTrunc
	 *		is true and when a shrink will truncate non-white
	 *		spaces.
	 */
	public DataValueDescriptor setWidth(int desiredWidth,
									int desiredScale, // Ignored
									boolean errorOnTrunc)
							throws StandardException
	{
		int sourceWidth;

		/*
		** If the input is NULL, nothing to do.
		*/
		if (getString() == null)
		{
			return this;
		}

		sourceWidth = getLength();

		/*
		** If the input is shorter than the desired type, construct a new
		** SQLChar padded with blanks to the right length.  Only
		** do this if we have a SQLChar -- SQLVarchars don't
		** pad.
		*/
		if (sourceWidth < desiredWidth)
		{
			if (!(this instanceof SQLVarchar))
			{
				StringBuffer	strbuf;

				strbuf = new StringBuffer(getString());
	
				for ( ; sourceWidth < desiredWidth; sourceWidth++)
				{
					strbuf.append(' ');
				}
	
				setValue(new String(strbuf));
			}
		}
		else if (sourceWidth > desiredWidth && desiredWidth > 0)
		{
			/*
			** Check whether any non-blank characters will be truncated.
			*/
			if (errorOnTrunc)
				hasNonBlankChars(getString(), desiredWidth, sourceWidth);
			//RESOLVE: should issue a warning instead

			/*
			** Truncate to the desired width.
			*/
			setValue(getString().substring(0, desiredWidth));
		}
		return this;
	}

	/*
	** SQL Operators
	*/

	/**
	 * The = operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the =
	 * @param right			The value on the right side of the =
	 *
	 * @return	A SQL boolean value telling whether the two parameters are equal
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue equals(DataValueDescriptor left,
							 DataValueDescriptor right)
								throws StandardException
	{
		boolean comparison;

		if ((left instanceof SQLChar) && (right instanceof SQLChar))
		{
			comparison = stringCompare((SQLChar) left, (SQLChar) right) == 0;
		}
		else
		{
			comparison = stringCompare(left.getString(),
									   right.getString()) == 0;
		}

		return SQLBoolean.truthValue(left,
									 right,
									 comparison);
	}

	/**
	 * The <> operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the <>
	 * @param right			The value on the right side of the <>
	 *
	 * @return	A SQL boolean value telling whether the two parameters
	 * are not equal
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue notEquals(DataValueDescriptor left,
							 DataValueDescriptor right)
								throws StandardException
	{
		boolean comparison;

		if ((left instanceof SQLChar) && (right instanceof SQLChar))
		{
			comparison = stringCompare((SQLChar) left, (SQLChar) right) != 0;
		}
		else
		{
			comparison = stringCompare(left.getString(),
									   right.getString()) != 0;
		}

		return SQLBoolean.truthValue(left,
									 right,
									 comparison);
	}

	/**
	 * The < operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the <
	 * @param right			The value on the right side of the <
	 *
	 * @return	A SQL boolean value telling whether the first operand is
	 *			less than the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue lessThan(DataValueDescriptor left,
							 DataValueDescriptor right)
								throws StandardException
	{
		boolean comparison;

		if ((left instanceof SQLChar) && (right instanceof SQLChar))
		{
			comparison = stringCompare((SQLChar) left, (SQLChar) right) < 0;
		}
		else
		{
			comparison = stringCompare(left.getString(),
									   right.getString()) < 0;
		}

		return SQLBoolean.truthValue(left,
									 right,
									 comparison);
	}

	/**
	 * The > operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the >
	 * @param right			The value on the right side of the >
	 *
	 * @return	A SQL boolean value telling whether the first operand is
	 *			greater than the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue greaterThan(DataValueDescriptor left,
							 DataValueDescriptor right)
								throws StandardException
	{
		boolean comparison;

		if ((left instanceof SQLChar) && (right instanceof SQLChar))
		{
			comparison = stringCompare((SQLChar) left, (SQLChar) right) > 0;
		}
		else
		{
			comparison = stringCompare(left.getString(),
									   right.getString()) > 0;
		}

		return SQLBoolean.truthValue(left,
									 right,
									 comparison);
	}

	/**
	 * The <= operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the <=
	 * @param right			The value on the right side of the <=
	 *
	 * @return	A SQL boolean value telling whether the first operand is
	 *			less than or equal to the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue lessOrEquals(DataValueDescriptor left,
							 DataValueDescriptor right)
								throws StandardException
	{
		boolean comparison;

		if ((left instanceof SQLChar) && (right instanceof SQLChar))
		{
			comparison = stringCompare((SQLChar) left, (SQLChar) right) <= 0;
		}
		else
		{
			comparison = stringCompare(left.getString(),
									   right.getString()) <= 0;
		}

		return SQLBoolean.truthValue(left,
									 right,
									 comparison);
	}

	/**
	 * The >= operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the >=
	 * @param right			The value on the right side of the >=
	 *
	 * @return	A SQL boolean value telling whether the first operand is
	 *			greater than or equal to the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue greaterOrEquals(DataValueDescriptor left,
							 DataValueDescriptor right)
								throws StandardException
	{
		boolean comparison;

		if ((left instanceof SQLChar) && (right instanceof SQLChar))
		{
			comparison = stringCompare((SQLChar) left, (SQLChar) right) >= 0;
		}
		else
		{
			comparison = stringCompare(left.getString(),
									   right.getString()) >= 0;
		}

		return SQLBoolean.truthValue(left,
									 right,
									 comparison);
	}

	/*
	** Concatable interface
	*/
	/**
	 * This method implements the char_length function for char.
	 *
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLInteger containing the length of the char value
	 *
	 * @exception StandardException		Thrown on error
	 *
	 * @see ConcatableDataValue#charLength(NumberDataValue)
	 */
	public NumberDataValue charLength(NumberDataValue result)
							throws StandardException
	{
		if (result == null)
		{
			result = new SQLInteger();
		}

		if (this.isNull())
		{
			result.setToNull();
			return result;
		}

		result.setValue(this.getLength());
		return result;
	}

	/**
	 * @see StringDataValue#concatenate
	 *
	 * @exception StandardException		Thrown on error
	 */
	public StringDataValue concatenate(
				StringDataValue leftOperand,
				StringDataValue rightOperand,
				StringDataValue result)
		throws StandardException
	{
		if (leftOperand.isNull() || leftOperand.getString() == null ||
			rightOperand.isNull() || rightOperand.getString() == null)
		{
			result.setToNull();
			return result;
		}

		result.setValue(leftOperand.getString().concat(rightOperand.getString()));
		return result;
	}


	/**
	 * This method implements the like function for char (with no escape value).
	 *
	 * @param value			The value to evaluate
	 * @param pattern		The pattern to use
	 *
	 * @return	A SQL boolean value telling whether the first operand is
	 *			like the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */
	public BooleanDataValue like(DataValueDescriptor pattern)
								throws StandardException
	{
		Boolean likeResult;

		if (! isNationalString())
		{
			// note that we call getLength() because the length
			// of the char array may be different than the
			// length we should be using (i.e. getLength()).
			// see getCharArray() for more info
			char[] evalCharArray = getCharArray();
			char[] patternCharArray = ((SQLChar)pattern).getCharArray();
			likeResult = Like.like(evalCharArray, 
								   getLength(),
 		    					   patternCharArray,
								   pattern.getLength());
		}
		else
		{
			SQLChar patternSQLChar = (SQLChar) pattern;
			likeResult = Like.like(getIntArray(), 
								   getIntLength(),
 		    					   patternSQLChar.getIntArray(),
								   patternSQLChar.getIntLength(),
								   getLocaleFinder().getCollator());
		}

		return SQLBoolean.truthValue(this,
									 pattern,
									 likeResult);
	}

	/**
	 * This method implements the like function for char with an escape value.
	 *
	 * @param pattern		The pattern to use
	 *
	 * @return	A SQL boolean value telling whether the first operand is
	 *			like the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue like(
							 DataValueDescriptor pattern,
							 DataValueDescriptor escape)
								throws StandardException
	{
		Boolean likeResult;

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(
							 pattern instanceof StringDataValue &&
							 escape instanceof StringDataValue,
			"All three operands must be instances of StringDataValue");

		// ANSI states a null escape yields 'unknown' results 
		//
		// This method is only called when we have an escape clause, so this 
		// test is valid

		if (escape.isNull())
		{
			throw StandardException.newException(SQLState.LANG_ESCAPE_IS_NULL);
		}

		if (! isNationalString())
		{
			// note that we call getLength() because the length
			// of the char array may be different than the
			// length we should be using (i.e. getLength()).
			// see getCharArray() for more info
			char[] evalCharArray = getCharArray();
			char[] patternCharArray = ((SQLChar)pattern).getCharArray();
			char[] escapeCharArray = (((SQLChar) escape).getCharArray());
			int escapeLength = escape.getLength();

			if (escapeCharArray != null && escapeLength != 1 )
			{
				throw StandardException.newException(SQLState.LANG_INVALID_ESCAPE_CHARACTER,
						new String(escapeCharArray));
			}
			else 
			{
			  // Make sure we fail for both varchar an nvarchar
			  // for multiple collation characters.
			  SQLChar escapeSQLChar = (SQLChar) escape;
			  int[] escapeIntArray = escapeSQLChar.getIntArray();
			  if (escapeIntArray != null && (escapeIntArray.length != 1))
			  {
				throw StandardException.newException(SQLState.LANG_INVALID_ESCAPE_CHARACTER,new String(escapeSQLChar.getCharArray()));
			  }
			}
			likeResult = Like.like(evalCharArray, 
								   getLength(),
 		    					   patternCharArray,
								   pattern.getLength(),
								   escapeCharArray,
								   escapeLength);
		}
		else
		{
			SQLChar patternSQLChar = (SQLChar) pattern;
			SQLChar escapeSQLChar = (SQLChar) escape;
			int[] escapeIntArray = escapeSQLChar.getIntArray();
			int escapeLength = escapeSQLChar.getIntLength();

			if (escapeIntArray != null && (escapeIntArray.length != 1))
			{
				throw StandardException.newException(SQLState.LANG_INVALID_ESCAPE_CHARACTER,
						new String(escapeSQLChar.getCharArray()));
			}
			likeResult = Like.like(getIntArray(), 
								   getIntLength(),
 		    					   patternSQLChar.getIntArray(),
								   patternSQLChar.getIntLength(),
								   escapeIntArray,
								   escapeLength,
								   getLocaleFinder().getCollator());
		}

		return SQLBoolean.truthValue(this,
									 pattern,
									 likeResult);
	}

    /**
     * This method implements the locate function for char.
     * @param searchFrom    - The string to search from
     * @param start         - The position to search from in string searchFrom
     * @param result        - The object to return
     *
     * Note: use getString() to get the string to search for.
     *
     * @return  The position in searchFrom the fist occurrence of this.value.
     *              0 is returned if searchFrom does not contain this.value.
     * @exception StandardException     Thrown on error
     */
    public NumberDataValue locate(  StringDataValue searchFrom,
                                    NumberDataValue start,
                                    NumberDataValue result)
                                    throws StandardException
    {
        int startVal;

        if( result == null )
        {
            result = new SQLInteger();
        }
        
        if( start.isNull() )
        {
            startVal = 1;
        }
        else
        {
            startVal = start.getInt();
        }

        if( isNull() || searchFrom.isNull() )
        {
            result.setToNull();
            return result;
        }

        String mySearchFrom = searchFrom.getString();
        String mySearchFor = this.getString();

        /* the below 2 if conditions are to emulate DB2's behavior */
        if( startVal < 1 )
        {
            throw StandardException.newException(
                    SQLState.LANG_INVALID_PARAMETER_FOR_SEARCH_POSITION, 
                    new String(getString()), new String(mySearchFrom), 
                    new Integer(startVal));
        }
        
        if( mySearchFor.length() == 0 )
        {
            result.setValue( startVal );
            return result;
        }

        result.setValue( mySearchFrom.indexOf(mySearchFor, startVal - 1) + 1);
        return result;
    }

	/**
	 * The SQL substr() function.
	 *
	 * @param start		Start of substr
	 * @param length	Length of substr
	 * @param result	The result of a previous call to this method,
	 *					null if not called yet.
	 * @param maxLen	Maximum length of the result
	 *
	 * @return	A ConcatableDataValue containing the result of the substr()
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ConcatableDataValue substring(
				NumberDataValue start,
				NumberDataValue length,
				ConcatableDataValue result,
				int maxLen)
		throws StandardException
	{
		int startInt;
		int lengthInt;
		StringDataValue stringResult;

		if (result == null)
		{
			result = getNewVarchar();
		}

		stringResult = (StringDataValue) result;

		/* The result is null if the receiver (this) is null or if the length is negative.
		 * We will return null, which is the only sensible thing to do.
		 * (If the user did not specify a length then length is not a user null.)
		 */
		if (this.isNull() || start.isNull() || (length != null && length.isNull()))
		{
			stringResult.setToNull();
			return stringResult;
		}

		startInt = start.getInt();

		// If length is not specified, make it till end of the string
		if (length != null)
		{
			lengthInt = length.getInt();
		}
		else lengthInt = maxLen - startInt + 1;

		/* DB2 Compatibility: Added these checks to match DB2. We currently enforce these
		 * limits in both modes. We could do these checks in DB2 mode only, if needed, so
		 * leaving earlier code for out of range in for now, though will not be exercised
		 */
		if ((startInt <= 0 || lengthInt < 0 || startInt > maxLen ||
				lengthInt > maxLen - startInt + 1))
			throw StandardException.newException(SQLState.LANG_SUBSTR_START_OR_LEN_OUT_OF_RANGE);
			
		// Return null if length is non-positive
		if (lengthInt < 0)
		{
			stringResult.setToNull();
			return stringResult;
		}

		/* If startInt < 0 then we count from the right of the string */
		if (startInt < 0)
		{
			// Return '' if window is to left of string.
			if (startInt + getLength() < 0 &&
				(startInt + getLength() + lengthInt <= 0))
			{
				stringResult.setValue("");
				return stringResult;
			}

			// Convert startInt to positive to get substring from right
			startInt += getLength();

			while (startInt < 0)
			{
				startInt++;
				lengthInt--;
			}
		}
		else if (startInt > 0)
		{
			/* java substring() is 0 based */
			startInt--;
		}

		/* Oracle docs don't say what happens if the window is to the
		 * left of the string.  Return "" if the window
		 * is to the left or right.
		 */
		if (lengthInt == 0 ||
			lengthInt <= 0 - startInt ||
			startInt > getLength())
		{
			stringResult.setValue("");
			return stringResult;
		}

		if (lengthInt >= getLength() - startInt)
		{
			stringResult.setValue(getString().substring(startInt));
		}
		else
		{
			stringResult.setValue(getString().substring(startInt, startInt + lengthInt));
		}

		return stringResult;
	}

	/**
	 * The SQL trim(), ltrim() and rtrim() functions.
	 *
	 * @param trimType	Type of trim
	 * @param result	The result of a previous call to this method,
	 *					null if not called yet.
	 *
	 * @return	A StringDataValue containing the result of the trim()
	 *
	 * @exception StandardException		Thrown on error
	 */
	public StringDataValue trim(
				int trimType,
				StringDataValue result)
		throws StandardException
	{

		if (result == null)
		{
			result = getNewVarchar();
		}

		/* The result is null if any of the parameters is a user null */
		if (this.isNull())
		{
			result.setToNull();
			return result;
		}

		char[] trimChars = {' '};
		String tmpValue = getString();

		// Trim leading characters if appropriate
		if (trimType == LEADING)
		{
			int start = 0;
			// Find the 1st character which doesn't get trimmed
			for ( ; start < tmpValue.length(); start++)
			{
				boolean found = false;
				for (int index = 0; index < trimChars.length; index++)
				{
					if (tmpValue.charAt(start) == trimChars[index])
					{
						found = true;
						break;
					}
				}

				if (! found)
				{
					break;
				}
			}

			// Trim if appropriate
			if (start == tmpValue.length())
			{
				tmpValue = "";
			}
			else if (start > 0)
			{
				tmpValue = tmpValue.substring(start);
			}
		}

		// Trim trailing characters if appropriate
		if (trimType == TRAILING)
		{
			int start = tmpValue.length();
			// Find the 1st character which doesn't get trimmed
			for ( ; start > 0; start--)
			{
				boolean found = false;
				for (int index = 0; index < trimChars.length; index++)
				{
					if (tmpValue.charAt(start - 1) == trimChars[index])
					{
						found = true;
						break;
					}
				}

				if (! found)
				{
					break;
				}
			}

			// Trim if appropriate
			if (start == 0)
			{
				tmpValue = "";
			}
			else if (start < tmpValue.length())
			{
				tmpValue = tmpValue.substring(0, start);
			}
		}
		
		result.setValue(tmpValue);
		return result;
	}

	/** @see StringDataValue#upper 
	 *
	 * @exception StandardException		Thrown on error
	 */
	public StringDataValue upper(StringDataValue result)
							throws StandardException
	{
		if (result == null)
		{
			result = (StringDataValue) getNewNull();
		}

		if (this.isNull())
		{
			result.setToNull();
			return result;
		}
		
		String upper = getString();
		upper = upper.toUpperCase(getLocale());
		result.setValue(upper);
		return result;
	}

	/** @see StringDataValue#lower 
	 *
	 * @exception StandardException		Thrown on error
	 */
	public StringDataValue lower(StringDataValue result)
							throws StandardException
	{
		if (result == null)
		{
			result = (StringDataValue) getNewNull();
		}

		if (this.isNull())
		{
			result.setToNull();
			return result;
		}

		
		String lower = getString();
		lower = lower.toLowerCase(getLocale());
		result.setValue(lower);
		return result;
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.CHAR_PRECEDENCE;
	}

	/**
	 * Compare two Strings using standard SQL semantics.
	 *
	 * @param op1				The first String
	 * @param op2				The second String
	 *
	 * @return  -1 - op1 <  op2
	 * 			 0 - op1 == op2
	 *			 1 - op1 > op2
	 */
	protected static int stringCompare(String op1, String op2)
	{
		int			posn;
		char		leftchar;
		char		rightchar;
		int			leftlen;
		int			rightlen;
		int			retvalIfLTSpace;
		String		remainingString;
		int			remainingLen;

		/*
		** By convention, nulls sort High, and null == null
		*/
		if (op1 == null || op2 == null)
		{
			if (op1 != null)	// op2 == null
				return -1;
			if (op2 != null)	// op1 == null
				return 1;
			return 0;			// both null
		}
		/*
		** Compare characters until we find one that isn't equal, or until
		** one String or the other runs out of characters.
		*/

		leftlen = op1.length();
		rightlen = op2.length();

		int shorterLen = leftlen < rightlen ? leftlen : rightlen;

		for (posn = 0; posn < shorterLen; posn++)
		{
			leftchar = op1.charAt(posn);
			rightchar = op2.charAt(posn);
			if (leftchar != rightchar)
			{
				if (leftchar < rightchar)
					return -1;
				else
					return 1;
			}
		}

		/*
		** All the characters are equal up to the length of the shorter
		** string.  If the two strings are of equal length, the values are
		** equal.
		*/
		if (leftlen == rightlen)
			return 0;

		/*
		** One string is shorter than the other.  Compare the remaining
		** characters in the longer string to spaces (the SQL standard says
		** that in this case the comparison is as if the shorter string is
		** padded with blanks to the length of the longer string.
		*/
		if (leftlen > rightlen)
		{
			/*
			** Remaining characters are on the left.
			*/

			/* If a remaining character is less than a space, return -1 (op1 < op2) */
			retvalIfLTSpace = -1;
			remainingString = op1;
			posn = rightlen;
			remainingLen = leftlen;
		}
		else
		{
			/*
			** Remaining characters are on the right.
			*/

			/* If a remaining character is less than a space, return 1 (op1 > op2) */
			retvalIfLTSpace = 1;
			remainingString = op2;
			posn = leftlen;
			remainingLen = rightlen;
		}

		/* Look at the remaining characters in the longer string */
		for ( ; posn < remainingLen; posn++)
		{
			char	remainingChar;

			/*
			** Compare the characters to spaces, and return the appropriate
			** value, depending on which is the longer string.
			*/

			remainingChar = remainingString.charAt(posn);

			if (remainingChar < ' ')
				return retvalIfLTSpace;
			else if (remainingChar > ' ')
				return -retvalIfLTSpace;
		}

		/* The remaining characters in the longer string were all spaces,
		** so the strings are equal.
		*/
		return 0;
	}

	/** 
	 * Compare two SQLChars.  This method will be overriden in the
	 * National char wrappers so that the appropriate comparison
	 * is done.
	 *
	 * @exception StandardException		Thrown on error
	 */
	 protected int stringCompare(SQLChar char1, SQLChar char2)
		 throws StandardException
	 {
		 return stringCompare(char1.getCharArray(), char1.getLength(), 
							  char2.getCharArray(), char2.getLength());
	 }

	/**
	 * Compare two Strings using standard SQL semantics.
	 *
	 * @param op1				The first String
	 * @param op2				The second String
	 *
	 * @return  -1 - op1 <  op2
	 * 			 0 - op1 == op2
	 *			 1 - op1 > op2
	 */
	protected static int stringCompare(char[] op1, int leftlen, char[] op2, int rightlen)
	{
		int			posn;
		char		leftchar;
		char		rightchar;
		int			retvalIfLTSpace;
		char[]		remainingString;
		int			remainingLen;

		/*
		** By convention, nulls sort High, and null == null
		*/
		if (op1 == null || op2 == null)
		{
			if (op1 != null)	// op2 == null
				return -1;
			if (op2 != null)	// op1 == null
				return 1;
			return 0;			// both null
		}
		/*
		** Compare characters until we find one that isn't equal, or until
		** one String or the other runs out of characters.
		*/
		int shorterLen = leftlen < rightlen ? leftlen : rightlen;
		for (posn = 0; posn < shorterLen; posn++)
		{
			leftchar = op1[posn];
			rightchar = op2[posn];
			if (leftchar != rightchar)
			{
				if (leftchar < rightchar)
					return -1;
				else
					return 1;
			}
		}

		/*
		** All the characters are equal up to the length of the shorter
		** string.  If the two strings are of equal length, the values are
		** equal.
		*/
		if (leftlen == rightlen)
			return 0;

		/*
		** One string is shorter than the other.  Compare the remaining
		** characters in the longer string to spaces (the SQL standard says
		** that in this case the comparison is as if the shorter string is
		** padded with blanks to the length of the longer string.
		*/
		if (leftlen > rightlen)
		{
			/*
			** Remaining characters are on the left.
			*/

			/* If a remaining character is less than a space, return -1 (op1 < op2) */
			retvalIfLTSpace = -1;
			remainingString = op1;
			posn = rightlen;
			remainingLen = leftlen;
		}
		else
		{
			/*
			** Remaining characters are on the right.
			*/

			/* If a remaining character is less than a space, return 1 (op1 > op2) */
			retvalIfLTSpace = 1;
			remainingString = op2;
			posn = leftlen;
			remainingLen = rightlen;
		}

		/* Look at the remaining characters in the longer string */
		for ( ; posn < remainingLen; posn++)
		{
			char	remainingChar;

			/*
			** Compare the characters to spaces, and return the appropriate
			** value, depending on which is the longer string.
			*/

			remainingChar = remainingString[posn];

			if (remainingChar < ' ')
				return retvalIfLTSpace;
			else if (remainingChar > ' ')
				return -retvalIfLTSpace;
		}

		/* The remaining characters in the longer string were all spaces,
		** so the strings are equal.
		*/
		return 0;
	}


	/**
	 * Compare a localized string with this one.
	 *
	 * @param str2              The other string
	 *
	 * @return  -1 - this <  str2
	 * 			 0 - this == str2
	 *			 1 - this > str2
	 */
	protected int stringCollatorCompare(SQLChar str2) 
		throws StandardException
	{	
		CollationKey ckey1 = this.getCollationKey();
		CollationKey ckey2 = str2.getCollationKey();

		
		/*
		** By convention, nulls sort High, and null == null
		*/
		if (ckey1 == null || ckey2 == null)
		{
			if (ckey1 != null)	// str2 == null
				return -1;
			if (ckey2 != null)	// this == null
				return 1;
			return 0;			// both == null
		}

		return ckey1.compareTo(ckey2);
	}
		
	protected CollationKey getCollationKey() throws StandardException
	{
		char tmpCharArray[];

		if (cKey != null)
			return cKey;

		if (rawLength == -1)
		{
			/* materialize the string if input is a stream */
			tmpCharArray = getCharArray();
			if (tmpCharArray == null)
				return null;
		}
		
		int lastNonspaceChar = rawLength;

		while (lastNonspaceChar > 0 && 
			   rawData[lastNonspaceChar - 1] == '\u0020')
			lastNonspaceChar--;			// count off the trailing spaces.

		RuleBasedCollator rbc = getLocaleFinder().getCollator();		
		cKey = rbc.getCollationKey(new String(rawData, 0, lastNonspaceChar));

		return cKey;
	}

	/*
	 * String display of value
	 */

	public String toString()
	{
		if (isNull()) {
			return "NULL";
		}

		if ((value == null) && (rawLength != -1)) {

			return new String(rawData, 0, rawLength);
		}

		if (stream != null) {
			try {
				return getString();
			} catch (Exception e) {
				return e.toString();
			}
		}

		return value;
	}

	/*
	 * Hash code
	 */
	public int hashCode()
	{
		try {
			if (getString() == null)
			{
				return 0;
			}
		}
		catch (StandardException se)
		{
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("Unexpected exception " + se);
			return 0;
		}


		/* value.hashCode() doesn't work because of the SQL blank padding behavior
		 * We want the hash code to be based on the value after the 
		 * trailing blanks have been trimmed.  Calling trim() is too expensive
		 * since it will create a new object, so here's what we do:
		 *		o  Walk from the right until we've found the 1st
		 *		   non-blank character.
		 *		o  Add up the characters from that character to the 1st in
		 *		   the string and return that as the hash code.
		 */
		int index;
		int hashcode = 0;

		// value will have been set by the getString() above
		String lvalue = value;

		// Find 1st non-blank from the right
		for (index = lvalue.length() - 1; 
		     index >= 0 && lvalue.charAt(index) == ' '; 
			 index--)
		{
			;
		}

		// Build the hash code
		for ( ; index >= 0; index--)
		{
			hashcode += lvalue.charAt(index);
		}

		return hashcode;
	}

	/**
	 * Implementation of hashCode() for the national character types,
	 * put here to make it accessible to all the national types.
	 */
	protected int nationalHashCode()
	{
		CollationKey tmpCKey = null;

		try
		{
			tmpCKey = getCollationKey();
		}
		catch (StandardException se)
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.THROWASSERT("Unexpected exception " + se);
			}
		}

		if (tmpCKey == null)
			return 0;
		return tmpCKey.hashCode();
	}

	private int[] getIntArray()
		throws StandardException
	{
		if (isNull())
		{
			return (int[]) null;
		}

		if (intArray != null)
		{
			return intArray;
		}

		// intLength should always be 0 when intArray is null
		if (SanityManager.DEBUG)
		{
			if (intLength != 0)
			{
				SanityManager.THROWASSERT(
					"intLength expected to be 0, not " + intLength);
			}
		}

		intArray = new int[getLength()];

		RuleBasedCollator rbc = getLocaleFinder().getCollator();
		CollationElementIterator cei = rbc.getCollationElementIterator(getString());
		int nextInt;
		while ((nextInt = cei.next()) != CollationElementIterator.NULLORDER)
		{
			/* Believe it or not, a String might have more
			 * collation elements than characters.
			 * So, we handle that case by increasing the int array
			 * by 5 and copying array elements.
			 */
			if (intLength == intArray.length)
			{
				int[] tempArray = intArray;
				intArray = new int[intLength + 5];
				for (int index = 0; index < tempArray.length; index++)
				{
					intArray[index] = tempArray[index];
				}
			}
			intArray[intLength++] = nextInt;
		}

		return intArray;
	}

	private int getIntLength()
	{
		return intLength;
	}

	/**
	 * Get a SQLVarchar for a built-in string function.  
	 * (Could be either a SQLVarchar or SQLNationalVarchar.)
	 *
	 * @return a SQLVarchar or SQLNationalVarchar.
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected StringDataValue getNewVarchar() throws StandardException
	{
		return new SQLVarchar();
	}

	/** 
	 * Return whether or not this is a national character datatype.
	 */
	protected boolean isNationalString()
	{
		return false;
	}

	/**
	 * This implements getDate() for the national types. It lives
	 * here so it can be shared between all the national types.
	 *
	 * @exception StandardException thrown on failure to convert
	 */
	protected Date	nationalGetDate( Calendar cal) throws StandardException
	{
		if (isNull())
            return null;
        SQLDate internalDate = new SQLDate( getString(), false, getLocaleFinder(), cal);
        return internalDate.getDate( cal);
	}

	/**
	 * This implements getTime() for the national types. It lives
	 * here so it can be shared between all the national types.
	 *
	 * @exception StandardException thrown on failure to convert
	 */
	protected Time nationalGetTime( Calendar cal) throws StandardException
	{
		if (isNull())
            return null;
        SQLTime internalTime = new SQLTime( getString(), false, getLocaleFinder(), cal);
        return internalTime.getTime( cal);
	}

	/**
	 * This implements getTimestamp() for the national types. It lives
	 * here so it can be shared between all the national types.
	 *
	 * @exception StandardException thrown on failure to convert
	 */
	protected Timestamp	nationalGetTimestamp( Calendar cal) throws StandardException
	{
        // DB2 does not support internationalized timestamps
        return getTimestamp( cal, getString(), getLocaleFinder());
	}

	protected void setLocaleFinder(LocaleFinder localeFinder)
	{
		this.localeFinder = localeFinder;
	}

	/** @exception StandardException		Thrown on error */
	private Locale getLocale() throws StandardException
	{
		return getLocaleFinder().getCurrentLocale();
	}

	protected LocaleFinder getLocaleFinder()
	{
		// This is not very satisfactory, as it creates a dependency on
		// the DatabaseContext. It's the best I could do on short notice,
		// though.  -  Jeff
		if (localeFinder == null)
		{
			DatabaseContext dc = (DatabaseContext) ContextService.getContext(DatabaseContext.CONTEXT_ID);
            if( dc != null)
                localeFinder = dc.getDatabase();
		}

		return localeFinder;
	}

	protected DateFormat getDateFormat() throws StandardException {
		return getLocaleFinder().getDateFormat();
	}
	protected DateFormat getTimeFormat() throws StandardException {
		return getLocaleFinder().getTimeFormat();
	}
	protected DateFormat getTimestampFormat() throws StandardException {
		return getLocaleFinder().getTimestampFormat();
	}

	protected DateFormat getDateFormat( Calendar cal) throws StandardException {
		return setDateFormatCalendar( getLocaleFinder().getDateFormat(), cal);
	}
	protected DateFormat getTimeFormat( Calendar cal) throws StandardException {
		return setDateFormatCalendar( getLocaleFinder().getTimeFormat(), cal);
	}
	protected DateFormat getTimestampFormat( Calendar cal) throws StandardException {
		return setDateFormatCalendar( getLocaleFinder().getTimestampFormat(), cal);
	}

    private DateFormat setDateFormatCalendar( DateFormat df, Calendar cal)
    {
        if( cal != null && df.getTimeZone() != cal.getTimeZone())
        {
            // The DateFormat returned by getDateFormat may be cached and used by other threads.
            // Therefore we cannot change its calendar.
            df = (DateFormat) df.clone();
            df.setCalendar( cal);
        }
        return df;
    }

	/*
	 * object state
	 */

	// Don't use value directly in most situations. Use getString()
	// OR use the rawData array if rawLength != -1.
	private		String	value;

	// rawData holds the reusable array for reading in
	// SQLChars. It contains a valid value if rawLength
	// is greater than or equal to 0. See getString() to see how it is
	// converted to a String. Even when converted to a String
	// object the rawData array remains for potential future
	// use, unless  rawLength is > 4096. In this case the
	// rawData is set to null to avoid huge memory use.
	private		char[]	rawData;
	private		int		rawLength = -1;

	// For null strings, cKey = null.
	private CollationKey cKey; 
	/*
	 * stream state
	 */
	protected InputStream stream;
	
	/* Comparison info for National subclasses) */
	private int[]	intArray;
	private int		intLength;

	/* Locale info (for International support) */
	private LocaleFinder localeFinder;

    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( SQLChar.class);

    public int estimateMemoryUsage()
    {
        int sz = BASE_MEMORY_USAGE + ClassSize.estimateMemoryUsage( value);
        if( null != rawData)
            sz += 2*rawData.length;
        // Assume that cKey, stream, and localFinder are shared, so do not count their memory usage
        if( null != intArray)
            sz += intArray.length*ClassSize.getIntSize();
        return sz;
    } // end of estimateMemoryUsage

	protected void copyState(SQLChar other) {

		this.value = other.value;
		this.rawData = other.rawData;
		this.rawLength = other.rawLength;
		this.cKey = other.cKey;
		this.stream = other.stream;
		this.intArray = intArray;
		this.intLength = intLength;
		this.localeFinder = localeFinder;
	}
}
