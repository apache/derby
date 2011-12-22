/*

   Derby - Class org.apache.derby.iapi.types.DataType

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
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.sanity.SanityManager;

import java.io.InputStream;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
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
    implements DataValueDescriptor, Comparable
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

	public int typeToBigDecimal() throws StandardException
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

    /**
     * Tells that the value isn't represented as a stream, which is true for
     * most Derby data types.
     * <p>
     * This method will be overridden by types able to use a stream as the
     * source.
     *
     * @return {@code false}
     */
    public boolean hasStream() {
        return false;
    }

    /**
     * Gets the value in the data stream descriptor as a trace string.
     * This default implementation simply forwards the call to
     * <code>getString</code>.
     *
     * @return The data value in a representation suitable for tracing.
     * @throws StandardException if getting the data value fails.
     * @see DataValueDescriptor#getString
     */
    public String getTraceString() throws StandardException {
        return getString();  
    }

    /**
     * Recycle this DataType object.
     *
     * @return this object with value set to null
     */
    public DataValueDescriptor recycle() {
        restoreToNull();
        return this;
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
	 */
	public void setValue(Date theValue, Calendar cal) throws StandardException
	{
		throwLangSetMismatch("java.sql.Date");
	}

	/**
	 * Set the value of this DataValueDescriptor.
	 * At DataType level just throws an error lower classes will override
	 *
	 * @param theValue	The Object value to set this DataValueDescriptor to
	 */
	public void setValue(Object theValue) throws StandardException
	{
		throwLangSetMismatch("java.lang.Object");
	}

	/**
	 * Set the value of this DataValueDescriptor.
	 * At DataType level just throws an error lower classes will override
	 *
	 * @param theValue	The BigDecimal value to set this DataValueDescriptor to
	 */
	public void setValue(String theValue) throws StandardException
	{
		throwLangSetMismatch("java.lang.String");
	}

	/**
	 * Set the value of this DataValueDescriptor.
	 * At DataType level just throws an error lower classes will override
	 *
	 * @param theValue	The Blob value to set this DataValueDescriptor to
	 */
	public void setValue(Blob theValue) throws StandardException
	{
		throwLangSetMismatch("java.sql.Blob");
	}
 
	/**
	 * Set the value of this DataValueDescriptor.
	 * At DataType level just throws an error lower classes will override
	 *
	 * @param theValue	The Clob value to set this DataValueDescriptor to
	 */
	public void setValue(Clob theValue) throws StandardException
	{
		throwLangSetMismatch("java.sql.Clob");
	}


    /**
	 * Set the value of this DataValueDescriptor to the given int value
	 * At DataType level just throws an error lower classes will override
	 *
	 * @param theValue	The value to set this DataValueDescriptor to
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
	 */
	public void setValue(byte[] theValue) throws StandardException
	{
		throwLangSetMismatch("byte[]");
	}

	/**
		Only to be called when the application sets a value using BigDecimal
	*/
	public void setBigDecimal(Number bigDecimal) throws StandardException
	{
		throwLangSetMismatch("java.math.BigDecimal");
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

	/**
	 * Set the value of this DataValueDescriptor based on the value
	 * of the specified DataValueDescriptor.
	 *
	 * @param dvd	The DataValueDescriptor that holds the value to
	 *  which we want to set this DataValueDescriptor's value.
	 *
	 */
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
	 * @see DataValueDescriptor#setObjectForCast
	 * 
	 * @exception StandardException
	 *                thrown on failure
	 * 
	 */
	public void setObjectForCast(Object theValue, boolean instanceOfResultType,
			String resultTypeClassName) throws StandardException {
		
		if (theValue == null)
		{
			setToNull();
			return;
		}
			
		/*
		 * Is the object of the right type? (only do the check if value is
		 * non-null
		 */
		if (!instanceOfResultType) {
				throw StandardException.newException(
						SQLState.LANG_DATA_TYPE_SET_MISMATCH,
						theValue.getClass().getName(), getTypeName(resultTypeClassName));
		}

		setObject(theValue);
	}
		
	/**
	 * Set the value from an non-null object. Usually overridden.
	 * This implementation throws an exception.
	 * The object will have been correctly typed from the call to setObjectForCast.
	 */
	void setObject(Object theValue)
		throws StandardException
	{
		genericSetObject(theValue);
	}
	
	/**
	 * Get the type name of this value, possibly overriding
	 * with the passed in class name (for user/java types).
	 * @param className
	 */
	String getTypeName(String className)
	{
		return getTypeName();
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


	void genericSetObject(Object theValue) throws StandardException {

		throwLangSetMismatch(theValue);
	}

    /**
     * Default implementation of shallow cloning, which forwards to the deep
     * clone method.
     * <p>
     * For many of the data types, a shallow clone will be the same as a deep
     * clone. The data types requiring special handling of shallow clones have
     * to override this method (for instance types whose value can be
     * represented as a stream).
     *
     * @return A shallow clone.
     */
    public DataValueDescriptor cloneHolder() {
		return cloneValue(false);
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
	 * Compare this Orderable with another, with configurable null ordering.
	 * The caller gets to determine how nulls
	 * should be treated - they can either be ordered values or unknown
	 * values. The caller also gets to decide, if they are ordered,
         * whether they should be lower than non-NULL values, or higher
	 *
	 * @param op	Orderable.ORDER_OP_EQUALS means do an = comparison.
	 *				Orderable.ORDER_OP_LESSTHAN means compare this < other.
	 *				Orderable.ORDER_OP_LESSOREQUALS means compare this <= other.
	 * @param other	The DataValueDescriptor to compare this one to.
	 * @param orderedNulls	True means to treat nulls as ordered values,
	 *						that is, treat SQL null as equal to null, and either greater or less
	 *						than all other values.
	 *						False means to treat nulls as unknown values,
	 *						that is, the result of any comparison with a null
	 *						is the UNKNOWN truth value.
         * @param nullsOrderedLow       True means NULL less than non-NULL,
         *                              false means NULL greater than non-NULL.
         *                              Only relevant if orderedNulls is true.
	 * @param unknownRV		The return value to use if the result of the
	 *						comparison is the UNKNOWN truth value.  In other
	 *						words, if orderedNulls is false, and a null is
	 *						involved in the comparison, return unknownRV.
	 *						This parameter is not used orderedNulls is true.
	 *
	 * @return	true if the comparison is true (duh!)
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean compare(int op,
						   DataValueDescriptor other,
						   boolean orderedNulls,
						   boolean nullsOrderedLow,
						   boolean unknownRV)
		throws StandardException
	{
		/* Use compare method from dominant type, flipping the operator
		 * to reflect flipping of sides.
		 */
		if (typePrecedence() < other.typePrecedence())
		{
			return other.compare(flip(op), this, orderedNulls,
                                nullsOrderedLow, unknownRV);
		}

		int result = compare(other, nullsOrderedLow);

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
	 * Compare this Orderable with another, with configurable null ordering.
	 * This method treats nulls as ordered values, but allows the caller
         * to specify whether they should be lower than all non-NULL values,
         * or higher than all non-NULL values.
	 *
	 * @param other		The Orderable to compare this one to.
         % @param nullsOrderedLow True if null should be lower than non-NULL
	 *
	 * @return  <0 - this Orderable is less than other.
	 * 			 0 - this Orderable equals other.
	 *			>0 - this Orderable is greater than other.
     *
     *			The code should not explicitly look for -1, or 1.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public int compare(DataValueDescriptor other, boolean nullsOrderedLow)
            throws StandardException
        {
            if (this.isNull() || other.isNull())
            {
                if (!isNull())
                    return nullsOrderedLow ? 1 : -1;
                if (!other.isNull())
                    return nullsOrderedLow ? -1 : 1;
                return 0; // both null
            }
            return compare(other);
        } 

	/**
	 * Wrapper method for the "compare(DataValueDescriptor)" method of
	 * this class.  Allows sorting of an array of DataValueDescriptors
	 * using the JVMs own sorting algorithm.  Currently used for
	 * execution-time sorting of IN-list values to allow proper handling
	 * (i.e. elimination) of duplicates.
	 *
	 * @see java.lang.Comparable#compareTo
	 */
	public int compareTo(Object otherDVD)
	{
		DataValueDescriptor other = (DataValueDescriptor)otherDVD;
		try {

			// Use compare method from the dominant type.
			if (typePrecedence() < other.typePrecedence())
				return (-1 * other.compare(this));

			return compare(other);

		} catch (StandardException se) {

			if (SanityManager.DEBUG)
			{
				SanityManager.THROWASSERT("Encountered error while " +
					"trying to compare two DataValueDescriptors: " +
					se.getMessage());
			}

			/* In case of an error in insane mode, just treat the
			 * values as "equal".
			 */
			return 0;
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
		 *
		 * NOTE: We may have sorted the IN-lst values at compile time using
		 * a specific (dominant) type, but we did *not* actually cast the
		 * values to that type.  So it's possible that different IN-list
		 * values have different precedences (verses each other and also
		 * verses the type of the left operand) when we get here.  Thus
		 * when we do any comparisons here we have to make sure we always
		 * compare using the dominant type of the two values being compared.
		 * Otherwise we can end up with wrong results when doing the binary
		 * search (ex. as caused by incorrect truncation).  DERBY-2256.
		 */
		int leftPrecedence = left.typePrecedence();
		DataValueDescriptor comparator = null;
		if (orderedList)
		{
			while (finish - start > 2)
			{
				int mid = ((finish - start) / 2) + start;
				comparator =
					(leftPrecedence < inList[mid].typePrecedence())
						? inList[mid]
						: left;

				// Search left
				retval = comparator.equals(left, inList[mid]);
				if (retval.equals(true))
				{
					return retval;
				}
				BooleanDataValue goLeft =
					comparator.greaterThan(inList[mid], left);
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
		 *
		 * Note: for the same reasons outlined above we must be sure to always
		 * do the comparisons using the dominant type of the two values being
		 * compared.
		 */
		for (int index = start; index < finish; index++)
		{
			comparator =
				(leftPrecedence < inList[index].typePrecedence())
					? inList[index]
					: left;

			retval = comparator.equals(left, inList[index]);
			if (retval.equals(true))
			{
				break;
			}

			// Can we stop searching?
			if (orderedList)
			{
				BooleanDataValue stop =
					comparator.greaterThan(inList[index], left);
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

	public void setValue(InputStream theStream, int valueLength) throws StandardException
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
		Return an conversion exception from this type to another.
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
