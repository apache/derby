/*

   Derby - Class org.apache.derby.iapi.types.DataType

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

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.CloneableObject;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.Orderable;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.iapi.services.i18n.LocaleFinder;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;

import java.util.Calendar;

/**
 *
 * DataType is the superclass for all data types. 
 * It provides common behavior
 * for datavalue descriptors -- it throws
 * exceptions for all of the get* and setvalue(*)  methods of
 * DataValueDescriptor; the subtypes need only
 * override the one for the type they represent
 * and all types it can also be returned as,
 * and the methods dealing with nulls.
 *
 * Since all types satisfy getString 
 * DataType does not define that
 * interfaces of DataValueDescriptor.
 *
 * DataType is a little glue for columns to hold
 * values with.
 *
 */
public abstract class DataType
	implements DataValueDescriptor, CloneableObject
{
	/*
	 * DataValueDescriptor Interface
	 */

	/**
	 * Gets the value in the data value descriptor as a boolean.
	 * Throws an exception if the data value is not receivable as a boolean.
	 *
	 * @return	The data value as a boolean.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean	getBoolean() throws StandardException
	{
		throw dataTypeConversion("boolean");
	}

	/**
	 * Gets the value in the data value descriptor as a byte.
	 * Throws an exception if the data value is not receivable as a byte.
	 *
	 * @return	The data value as a byte.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public byte	getByte() throws StandardException
	{
		throw dataTypeConversion("byte");
	}

	/**
	 * Gets the value in the data value descriptor as a short.
	 * Throws an exception if the data value is not receivable as a short.
	 *
	 * @return	The data value as a short.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public short	getShort() throws StandardException
	{
		throw dataTypeConversion("short");
	}

	/**
	 * Gets the value in the data value descriptor as a int.
	 * Throws an exception if the data value is not receivable as a int.
	 *
	 * @return	The data value as a int.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public int	getInt() throws StandardException
	{
		throw dataTypeConversion("int");
	}

	/**
	 * Gets the value in the data value descriptor as a long.
	 * Throws an exception if the data value is not receivable as a long.
	 *
	 * @return	The data value as a long.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public long	getLong() throws StandardException
	{
		throw dataTypeConversion("long");
	}

	/**
	 * Gets the value in the data value descriptor as a float.
	 * Throws an exception if the data value is not receivable as a float.
	 *
	 * @return	The data value as a float.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public float	getFloat() throws StandardException
	{
		throw dataTypeConversion("float");
	}

	/**
	 * Gets the value in the data value descriptor as a double.
	 * Throws an exception if the data value is not receivable as a double.
	 *
	 * @return	The data value as a double.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public double	getDouble() throws StandardException
	{
		throw dataTypeConversion("double");
	}

	/**
	 * Gets the value in the data value descriptor as a BigDecimal.
	 * Throws an exception if the data value is not receivable as a BigDecimal.
	 *
	 * @return	The data value as a java.lang.BigDecimal.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public BigDecimal	getBigDecimal() throws StandardException
	{
		throw dataTypeConversion("java.math.BigDecimal");
	}

	/**
	 * Gets the value in the data value descriptor as a byte[].
	 * Throws an exception if the data value is not receivable as a Binary or Varbinary.
	 *
	 * @return	The Binary value as a byte[].
	 *
	 * @exception StandardException		Thrown on error
	 */
	public byte[]	getBytes() throws StandardException
	{
		throw dataTypeConversion("byte[]");
	}

	/**
	 * Gets the value in the data value descriptor as a java.sql.Date.
	 * Throws an exception if the data value is not receivable as a Date.
     *	@param cal calendar for object creation
	 * @return	The data value as a java.sql.Date.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public Date	getDate( Calendar cal) throws StandardException
	{
		throw dataTypeConversion("java.sql.Date");
	}

	/**
	 * Gets the value in the data value descriptor as a java.sql.Time.
	 * Throws an exception if the data value is not receivable as a Time.
     *	@param cal calendar for object creation
	 * @return	The data value as a java.sql.Time.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public Time	getTime( Calendar cal) throws StandardException
	{
		throw dataTypeConversion("java.sql.Time");
	}

	/**
	 * Gets the value in the data value descriptor as a java.sql.Timestamp.
	 * Throws an exception if the data value is not receivable as a Timestamp.
     *	@param cal calendar for object creation
	 * @return	The data value as a java.sql.Timestamp.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public Timestamp	getTimestamp( Calendar cal) throws StandardException
	{
		throw dataTypeConversion("java.sql.Timestamp");
	}

	/**
	 * Gets the value in the data stream descriptor as an InputStream.
	 * Throws an exception if the data value is not receivable as a stream.
	 *
	 * @return	The data value as an InputStream.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public InputStream	getStream() throws StandardException
	{
		throw dataTypeConversion( 
			MessageService.getTextMessage(SQLState.LANG_STREAM));
	}

	/*
	 * Column interface
	 */
	
	/**
	 * The is null operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 *
	 * @return	A SQL boolean value telling whether the operand is null
	 *
	 */

	public final BooleanDataValue isNullOp()
	{
		return SQLBoolean.truthValue(isNull());
	}

	/**
	 * The is not null operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 *
	 * @return	A SQL boolean value telling whether the operand is not null
	 *
	 */

	public final BooleanDataValue isNotNull()
	{
		return SQLBoolean.truthValue(!isNull());
	}

	/**
	 * Set the value of this DataValueDescriptor.
	 * At DataType level just throws an error lower classes will override
	 *
	 * @param theValue	The Time value to set this DataValueDescriptor to
	 *
	 * @return	This DataValueDescriptor
	 *
	 */
	public void setValue(Time theValue) throws StandardException
	{
        setValue( theValue, (Calendar) null);
	}

	/**
	 * Set the value of this DataValueDescriptor.
	 * At DataType level just throws an error lower classes will override
	 *
	 * @param theValue	The Time value to set this DataValueDescriptor to
     * @param cal The time zone from the calendar is used to construct the database time value
	 *
	 * @return	This DataValueDescriptor
	 *
	 */
	public void setValue(Time theValue, Calendar cal) throws StandardException
	{
		throwLangSetMismatch("java.sql.Time");
	}
	
	/**
	 * Set the value of this DataValueDescriptor.
	 * At DataType level just throws an error lower classes will override
	 *
	 * @param theValue	The Timestamp value to set this DataValueDescriptor to
	 *
	 * @return	This DataValueDescriptor
	 *
	 */
	public void setValue(Timestamp theValue) throws StandardException
	{
        setValue( theValue, (Calendar) null);
	}
	
	/**
	 * Set the value of this DataValueDescriptor.
	 * At DataType level just throws an error lower classes will override
	 *
	 * @param theValue	The Timestamp value to set this DataValueDescriptor to
     * @param cal The time zone from the calendar is used to construct the database timestamp value
	 *
	 * @return	This DataValueDescriptor
	 *
	 */
	public void setValue(Timestamp theValue, Calendar cal) throws StandardException
	{
		throwLangSetMismatch("java.sql.Timestamp");
	}
	
	/**
	 * Set the value of this DataValueDescriptor.
	 * At DataType level just throws an error lower classes will override
	 *
	 * @param theValue	The Date value to set this DataValueDescriptor to
	 *
	 * @return	This DataValueDescriptor
	 *
	 */
	public void setValue(Date theValue) throws StandardException
	{
        setValue( theValue, (Calendar) null);
	}
	
	/**
	 * Set the value of this DataValueDescriptor.
	 * At DataType level just throws an error lower classes will override
	 *
	 * @param theValue	The Date value to set this DataValueDescriptor to
     * @param cal The time zone from the calendar is used to construct the database date value
	 *
	 * @return	This DataValueDescriptor
	 *
	 */
	public void setValue(Date theValue, Calendar cal) throws StandardException
	{
		throwLangSetMismatch("java.sql.Date");
	}
	
	/**
	 * Set the value of this DataValueDescriptor.
	 * At DataType level just throws an error lower classes will override
	 *
	 * @param theValue	The BigDecimal value to set this DataValueDescriptor to
	 *
	 * @return	This DataValueDescriptor
	 *
	 */

	public void setValue(BigDecimal theValue) throws StandardException
	{
		throwLangSetMismatch("java.math.BigDecimal");
	}


	/**
	 * Set the value of this DataValueDescriptor.
	 * At DataType level just throws an error lower classes will override
	 *
	 * @param theValue	The BigDecimal value to set this DataValueDescriptor to
	 *
	 * @return	This DataValueDescriptor
	 *
	 */
	public void setValue(String theValue) throws StandardException
	{
		throwLangSetMismatch("java.lang.String");
	}


    /**
	 * Set the value of this DataValueDescriptor to the given int value
	 * At DataType level just throws an error lower classes will override
	 *
	 * @param theValue	The value to set this DataValueDescriptor to
	 *
	 * @return	This DataValueDescriptor
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setValue(int theValue) throws StandardException
	{
		throwLangSetMismatch("int");
	}

	/**
	 * Set the value of this DataValueDescriptor to the given double value
	 * At DataType level just throws an error lower classes will override
	 *
	 * @param theValue	The value to set this DataValueDescriptor to
	 *
	 * @return	This DataValueDescriptor
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setValue(double theValue) throws StandardException
	{
		throwLangSetMismatch("double");
	}

	/**
	 * Set the value of this DataValueDescriptor to the given float value
	 * At DataType level just throws an error lower classes will override
	 *
	 * @param theValue	The value to set this DataValueDescriptor to
	 *
	 * @return	This DataValueDescriptor
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setValue(float theValue) throws StandardException
	{
		throwLangSetMismatch("float");
	}
 
	/**
	 * Set the value of this DataValueDescriptor to the given short value
	 * At DataType level just throws an error lower classes will override
	 *
	 * @param theValue	The value to set this DataValueDescriptor to
	 *
	 * @return	This DataValueDescriptor
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setValue(short theValue) throws StandardException
	{
		throwLangSetMismatch("short");
	}
	/**
	 * Set the value of this DataValueDescriptor to the given long value
	 * At DataType level just throws an error lower classes will override
	 *
	 * @param theValue	The value to set this DataValueDescriptor to
	 *
	 * @return	This DataValueDescriptor
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setValue(long theValue) throws StandardException
	{
		throwLangSetMismatch("long");
	}

	/**
	 * Set the value of this DataValueDescriptor to the given byte value
	 * At DataType level just throws an error lower classes will override
	 *
	 * @param theValue	The value to set this DataValueDescriptor to
	 *
	 * @return	This DataValueDescriptor
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setValue(byte theValue) throws StandardException
	{
		throwLangSetMismatch("byte");
	}

	/**
	 * Set the value.
	 * At DataType level just throws an error lower classes will override
	 *
	 * @param theValue	Contains the boolean value to set this to
	 *
	 * @return	This value
	 *
	 */
	public void setValue(boolean theValue) throws StandardException
	{
		throwLangSetMismatch("boolean");
	}

	/**
	 * Set the value of this DataValueDescriptor.
	 * At DataType level just throws an error lower classes will override
	 *
	 * @param theValue	The byte value to set this DataValueDescriptor to
	 *
	 * @return	This DataValueDescriptor
	 *
	 */
	public void setValue(byte[] theValue) throws StandardException
	{
		throwLangSetMismatch("byte[]");
	}


	public final void setValue(DataValueDescriptor dvd) throws StandardException {

		if (dvd.isNull())
		{
			setToNull();
			return;
		}

		try {
			setFrom(dvd);
		} catch (StandardException se) {
			String msgId = se.getMessageId();

			if (SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE.equals(msgId))
				throw outOfRange();

			if (SQLState.LANG_FORMAT_EXCEPTION.equals(msgId))
				throw invalidFormat();

			throw se;

		}
	}

	protected void setFrom(DataValueDescriptor dvd) throws StandardException
	{
		throw StandardException.newException(SQLState.NOT_IMPLEMENTED);
	}

	/**
	 * @see DataValueDescriptor#setToNull
	 */
	 public void setToNull()
	 {
	 	restoreToNull();
	 }

	/**
	 * Set the Object that this Data Type contains (for an explicit cast).
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setObjectForCast(Object value, boolean instanceOf, String resultTypeClassName) throws StandardException
	{
		setValue(value);
	}

	/**
		Set the value from an object.
		Usually overridden. This implementation sets this to
		NULL if the passed in value is null, otherwise an exception
		is thrown.
	*/
	public void setValue(Object theValue)
		throws StandardException
	{
		if (theValue == null)
			setToNull();
		else
			throwLangSetMismatch(theValue);
	}

	/**
	 * Gets the value in the data value descriptor as a int.
	 * Throws an exception if the data value is not receivable as a int.
	 *
	 * @return	The data value as a int.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public Object	getObject() throws StandardException
	{
		throw dataTypeConversion("java.lang.Object");
	}


	protected void genericSetObject(Object theValue) throws StandardException {

		//if (theValue instanceof String)
		//	((DataValueDescriptor) this).setValue((String) theValue);
		//else
			throwLangSetMismatch(theValue);
	}

	/**
	 * From CloneableObject
	 *
	 * @return clone of me as an Object
	 */
	public Object cloneObject()
	{
		return getClone();
	}

	// International support

	/**
	 * International version of getString(). Overridden for date, time,
	 * and timestamp in SQLDate, SQLTime, SQLTimestamp.
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected String getNationalString(LocaleFinder localeFinder) throws StandardException
	{
		return getString();
	}

	public void throwLangSetMismatch(Object value) throws StandardException {
		throwLangSetMismatch(value.getClass().getName());
	}

	void throwLangSetMismatch(String argTypeName) throws StandardException
	{
		throw StandardException.newException(SQLState.LANG_DATA_TYPE_SET_MISMATCH, 
									   argTypeName, this.getTypeName());
		
	}

	public void setInto(PreparedStatement ps, int position) throws SQLException, StandardException {

		ps.setObject(position, getObject());
	}

	/**
		Set this value into a ResultSet for a subsequent ResultSet.insertRow
		or ResultSet.updateRow. This method will only be called for non-null values.

		@exception SQLException thrown by the ResultSet object
		@exception StandardException thrown by me accessing my value.
	*/
	public void setInto(ResultSet rs, int position) throws SQLException, StandardException {
		rs.updateObject(position, getObject());
	}


	/**
	 * Default normalization method. No information needed from DataTypeDescriptor.
	 *
	 * @param desiredType	The type to normalize the source column to
	 * @param source		The value to normalize
	 *
	 *
	 * @exception StandardException				Thrown normalization error.
	 */

	public void normalize(
				DataTypeDescriptor desiredType,
				DataValueDescriptor source)
					throws StandardException
	{
		((DataValueDescriptor) this).setValue(source);
	}
	/**
	 * Each built-in type in JSQL has a precedence.  This precedence determines
	 * how to do type promotion when using binary operators.  For example, float
	 * has a higher precedence than int, so when adding an int to a float, the
	 * result type is float.
	 *
	 * The precedence for some types is arbitrary.  For example, it doesn't
	 * matter what the precedence of the boolean type is, since it can't be
	 * mixed with other types.  But the precedence for the number types is
	 * critical.  The SQL standard requires that exact numeric types be
	 * promoted to approximate numeric when one operator uses both.  Also,
	 * the precedence is arranged so that one will not lose precision when
	 * promoting a type.
	 *
	 * @return		The precedence of this type.
	 */
	public int					typePrecedence() {
		return -1;
	}

	/**
	 * The = operator as called from the language module, as opposed to
	 * the storage module. This default implementations uses compare().
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
		return SQLBoolean.truthValue(left,
									 right,
									 left.compare(right) == 0);
	}

	/**
	 * The <> operator as called from the language module, as opposed to
	 * the storage module. This default implementations uses compare().
	 *
	 * @param left			The value on the left side of the <>
	 * @param right			The value on the right side of the <>
	 *
	 * @return	A SQL boolean value telling whether the two parameters
	 *			are not equal
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue notEquals(DataValueDescriptor left,
							DataValueDescriptor right)
			throws StandardException
	{
		return SQLBoolean.truthValue(left,
									 right,
									 left.compare(right) != 0);
	}
	/**
	* The < operator as called from the language module, as opposed to
	* the storage module.
	*
	* @param left   The value on the left side of the <
	* @param right   The value on the right side of the <
	*
	* @return A SQL boolean value telling whether the first operand is less
	*   than the second operand
	*
	* @exception StandardException  Thrown on error
	*/

	public BooleanDataValue lessThan(DataValueDescriptor left,
	   DataValueDescriptor right)
	throws StandardException
	{
		return SQLBoolean.truthValue(left,
		  right,
		  left.compare(right) < 0);
	}
	/**
	 * The > operator as called from the language module, as opposed to
	 * the storage module. This default implementations uses compare().
	 *
	 * @param left			The value on the left side of the >
	 * @param right			The value on the right side of the >
	 *
	 * @return	A SQL boolean value telling whether the first operand is greater
	 *			than the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue greaterThan(DataValueDescriptor left,
							DataValueDescriptor right)
			throws StandardException
	{
		return SQLBoolean.truthValue(left,
									 right,
									 left.compare(right) > 0);
	}

	/**
	 * The <= operator as called from the language module, as opposed to
	 * the storage module. This default implementations uses compare().
	 *
	 * @param left			The value on the left side of the <=
	 * @param right			The value on the right side of the <=
	 *
	 * @return	A SQL boolean value telling whether the first operand is less
	 *			than or equal to the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue lessOrEquals(DataValueDescriptor left,
							DataValueDescriptor right)
			throws StandardException
	{
		return SQLBoolean.truthValue(left,
									 right,
									 left.compare(right) <= 0);
	}

	/**
	 * The >= operator as called from the language module, as opposed to
	 * the storage module. This default implementation uses compare().
	 *
	 * @param left			The value on the left side of the >=
	 * @param right			The value on the right side of the >=
	 *
	 * @return	A SQL boolean value telling whether the first operand is greater
	 *			than or equal to the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue greaterOrEquals(DataValueDescriptor left,
							DataValueDescriptor right)
			throws StandardException
	{
		return SQLBoolean.truthValue(left,
									 right,
									 left.compare(right) >= 0);
	}
	public boolean compare(int op,
						   DataValueDescriptor other,
						   boolean orderedNulls,
						   boolean unknownRV)
		throws StandardException
	{
		/* Use compare method from dominant type, flipping the operator
		 * to reflect flipping of sides.
		 */
		if (typePrecedence() < other.typePrecedence())
		{
			return other.compare(flip(op), this, orderedNulls, unknownRV);
		}

		int result = compare(other);

		switch(op)
		{
		case ORDER_OP_LESSTHAN:
			return (result < 0);   // this <  other
		case ORDER_OP_EQUALS:
			return (result == 0);  // this == other
		case ORDER_OP_LESSOREQUALS:
			return (result <= 0);  // this <= other
		// flipped operators
		case ORDER_OP_GREATERTHAN:
			return (result > 0);   // this > other
		case ORDER_OP_GREATEROREQUALS:
			return (result >= 0);  // this >= other
		default:
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("Invalid Operator");
			return false;
		}
	}

	/**
	 * Flip the operator used in a comparison (< -> >).
	 * This is useful when flipping a comparison due to
	 * type precedence.
	 * 
	 * @param operator	The operator to flip.
	 * 
	 * @return The flipped operator.
	 */
	protected static int flip(int operator)
	{
		switch (operator)
		{
			case Orderable.ORDER_OP_LESSTHAN:
				// < -> > 
				return Orderable.ORDER_OP_GREATERTHAN;
			case Orderable.ORDER_OP_LESSOREQUALS:
				// <= -> >= 
				return Orderable.ORDER_OP_GREATEROREQUALS;
			case Orderable.ORDER_OP_EQUALS:
				// = -> = 
				return Orderable.ORDER_OP_EQUALS;
			default:
				// These operators only appear due to flipping.
				// They should never be flipped themselves.
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
						"Attempting to flip an operator that is not " +
						"expected to be flipped.");
				}
				return operator;
		}
	}

	/*
	 * DataValueDescriptor interface
	 */

	/**
	 * @see DataValueDescriptor#coalesce
	 * @exception StandardException		Thrown on error
	 */
	public DataValueDescriptor coalesce(DataValueDescriptor[] argumentsList, DataValueDescriptor returnValue)
						throws StandardException
	{
		// arguments list should have at least 2 arguments
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(argumentsList != null,
				"argumentsList expected to be non-null");
			SanityManager.ASSERT(argumentsList.length > 1,
				"argumentsList.length expected to be > 1");
		}

		/* Walk the arguments list until we find a non-null value. Otherwise we will return null
		 */
		int index;
		for (index = 0; index < argumentsList.length; index++)
		{
			if (!(argumentsList[index].isNull()))
			{
				returnValue.setValue(argumentsList[index]);
				return returnValue;
			}
		}

		returnValue.setToNull();
		return returnValue;

	}

	/**
	 * @see DataValueDescriptor#in
	 * @exception StandardException		Thrown on error
	 */
	public BooleanDataValue in(DataValueDescriptor left,
							   DataValueDescriptor[] inList,
							   boolean orderedList) 
						throws StandardException
	{
		BooleanDataValue retval = null;

		// in list should be non-empty
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(inList != null,
				"inList expected to be non-null");
			SanityManager.ASSERT(inList.length > 0,
				"inList.length expected to be > 0");
		}

		// if left is null then just return false
		if (left.isNull())
		{
			return SQLBoolean.truthValue(left,
									 inList[0],
									 false);
		}

		int start = 0;
		int finish = inList.length;

		/* Do a binary search if the list is ordered until the 
		 * range of values to search is 3 or less.
		 * NOTE: We've ensured that the IN list and the left all have
		 * the same precedence at compile time.  If we don't enforce 
		 * the same precendence then
		 * we could get the wrong result when doing a binary search.
		 */
		if (orderedList)
		{
			while (finish - start > 2)
			{
				int mid = ((finish - start) / 2) + start;
				// Search left
				retval = equals(left, inList[mid]);
				if (retval.equals(true))
				{
					return retval;
				}
				BooleanDataValue goLeft = greaterThan(inList[mid], left);
				if (goLeft.equals(true))
				{
					// search left
					finish = mid;
				}
				else
				{
					// search right
					start = mid;
				}
			}
		}

		/* Walk the in list comparing the values.  Return as soon as we
		 * find a match.  If the list is ordered, return as soon as the left
		 * value is greater than an element in the in list.
		 */
		for (int index = start; index < finish; index++)
		{
			retval = equals(left, inList[index]);
			if (retval.equals(true))
			{
				break;
			}

			// Can we stop searching?
			if (orderedList)
			{
				BooleanDataValue stop = greaterThan(inList[index], left);
				if (stop.equals(true))
				{
					break;
				}
			}
		}

		return retval;
	}

	/*
	 * equals
	 */
	public boolean equals(Object other)
	{
		if (! (other instanceof DataValueDescriptor))
		{
			return false;
		}

		try
		{
			return compare(ORDER_OP_EQUALS, (DataValueDescriptor) other, true, false);
		}
		catch (StandardException se)
		{
			return false;
		}
	}

	public void setValue(InputStream theStream, int streamLength) throws StandardException
	{
		throwLangSetMismatch("java.io.InputStream");
	}

	/**
		Check the value to seem if it conforms to the restrictions
		imposed by DB2/JCC on host variables for this type.

		@exception StandardException Variable is too big.
	*/
	public void checkHostVariable(int declaredLength) throws StandardException
	{
	}


	/**
		Return an conversion exception for this type.
	*/
	protected final StandardException dataTypeConversion(String targetType) {
		return StandardException.newException(SQLState.LANG_DATA_TYPE_GET_MISMATCH, 
			targetType, this.getTypeName());

	}

	/**
		Return an out of range exception for this type.
	*/
	protected final StandardException outOfRange()
	{
		return StandardException.newException(
				SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, getTypeName());
	}

	/**
		Return an out of range exception for this type.
	*/
	protected final StandardException invalidFormat()
	{
		return StandardException.newException(
				SQLState.LANG_FORMAT_EXCEPTION, getTypeName());
	}
}
