/*

   Derby - Class org.apache.derby.iapi.types.DataValueDescriptor

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

import org.apache.derby.iapi.services.io.ArrayInputStream;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.io.Storable;

import java.io.InputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.SQLException;
import java.util.Calendar;

/**
 * The DataValueDescriptor interface provides methods to get the data from
 * a column returned by a statement.
 * <p>
 * This interface matches the getXXX methods on java.sql.ResultSet. This means
 * everyone satisfies getString and getObject; all of the numeric types, within
 * the limits of representation, satisfy all of the numeric getXXX methods;
 * all of the character types satisfy all of the getXXX methods except
 * getBytes and getBinaryStream; all of the binary types satisfy getBytes and
 * all of the getXXXStream methods; Date satisfies getDate and getTimestamp;
 * Time satisfies getTime; and Timestamp satisfies all of the date/time getXXX 
 * methods.
 * The "preferred" method (one that will always work, I presume) is the one that
 * matches the type most closely. See the comments below for
 * "preferences".  See the JDBC guide for details.
 * <p>
 * This interface does not include the getXXXStream methods.
 * <p>
 * The preferred methods for JDBC are:
 * <p>
 * CHAR and VARCHAR - getString()
 * <p>
 * BIT - getBoolean()
 * <p>
 * TINYINT - getByte()
 * <p>
 * SMALLINT - getShort()
 * <p>
 * INTEGER - getInt()
 * <p>
 * BIGINT - getLong()
 * <p>
 * REAL - getFloat()
 * <p>
 * FLOAT and DOUBLE - getDouble()
 * <p>
 * DECIMAL and NUMERIC - getBigDecimal()
 * <p>
 * BINARY and VARBINARY - getBytes()
 * <p>
 * DATE - getDate()
 * <p>
 * TIME - getTime()
 * <p>
 * TIMESTAMP - getTimestamp()
 * <p>
 * No JDBC type corresponds to getObject().  Use this for user-defined types
 * or to get the JDBC types as java Objects.  All primitive types will be
 * wrapped in their corresponding Object type, i.e. int will be
 * wrapped in an Integer.
 * <p>
 * getStream() 
 * 
 */

public interface DataValueDescriptor extends Storable, Orderable
{

    /**
     * Constant indicating that the logical length of a value (i.e. chars for
     * string types or bytes for binary types) is unknown.
     */
    int UNKNOWN_LOGICAL_LENGTH = -1;

	/**
	 * Gets the length of the data value.  The meaning of this is
	 * implementation-dependent.  For string types, it is the number of
	 * characters in the string.  For numeric types, it is the number of
	 * bytes used to store the number.  This is the actual length
	 * of this value, not the length of the type it was defined as.
	 * For example, a VARCHAR value may be shorter than the declared
	 * VARCHAR (maximum) length.
	 *
	 * @return	The length of the data value
	 *
	 * @exception StandardException   On error
	 */
	int	getLength() throws StandardException;

	/**
	 * Gets the value in the data value descriptor as a String.
	 * Throws an exception if the data value is not a string.
	 *
	 * @return	The data value as a String.
	 *
	 * @exception StandardException   Thrown on error
	 */
	String	getString() throws StandardException;

    /**
     * Gets the value in the data value descriptor as a trace string.
     * If the value itself is not suitable for tracing purposes, a more
     * suitable representation is returned. For instance, data values
     * represented as streams are not materialized. Instead, information about
     * the associated stream is given.
     */
    String getTraceString() throws StandardException;

	/**
	 * Gets the value in the data value descriptor as a boolean.
	 * Throws an exception if the data value is not a boolean.
	 * For DataValueDescriptor, this is the preferred interface
	 * for BIT, but for this no-casting interface, it isn't, because
	 * BIT is stored internally as a Bit, not as a Boolean.
	 *
	 * @return	The data value as a boolean.
	 *
	 * @exception StandardException   Thrown on error
	 */
	boolean	getBoolean() throws StandardException;

	/**
	 * Gets the value in the data value descriptor as a byte.
	 * Throws an exception if the data value is not a byte.
	 *
	 * @return	The data value as a byte.
	 *
	 * @exception StandardException   Thrown on error
	 */
	byte	getByte() throws StandardException;

	/**
	 * Gets the value in the data value descriptor as a short.
	 * Throws an exception if the data value is not a short.
	 *
	 * @return	The data value as a short.
	 *
	 * @exception StandardException   Thrown on error
	 */
	short	getShort() throws StandardException;

	/**
	 * Gets the value in the data value descriptor as an int.
	 * Throws an exception if the data value is not an int.
	 *
	 * @return	The data value as a int.
	 *
	 * @exception StandardException   Thrown on error
	 */
	int	getInt() throws StandardException;

	/**
	 * Gets the value in the data value descriptor as a long.
	 * Throws an exception if the data value is not a long.
	 *
	 * @return	The data value as a long.
	 *
	 * @exception StandardException   Thrown on error
	 */
	long	getLong() throws StandardException;

	/**
	 * Gets the value in the data value descriptor as a float.
	 * Throws an exception if the data value is not a float.
	 *
	 * @return	The data value as a float.
	 *
	 * @exception StandardException   Thrown on error
	 */
	float	getFloat() throws StandardException;

	/**
	 * Gets the value in the data value descriptor as a double.
	 * Throws an exception if the data value is not a double.
	 *
	 * @return	The data value as a double.
	 *
	 * @exception StandardException   Thrown on error
	 */
	double	getDouble() throws StandardException;
	
	/**
	 * How should this value be obtained so that it can
	 * be converted to a BigDecimal representation.
	 * @return Types.CHAR for String conversion through getString
	 * Types.DECIMAL for BigDecimal through getObject or Types.BIGINT
	 * for long conversion through getLong
	 * @exception StandardException Conversion is not possible
	 */
	int typeToBigDecimal() throws StandardException;

	/**
	 * Gets the value in the data value descriptor as a byte array.
	 * Throws an exception if the data value is not a byte array.
	 *
	 * @return	The data value as a byte[].
	 *
	 * @exception StandardException  Thrown on error
	 */
	byte[]	getBytes() throws StandardException;

	/**
	 * Gets the value in the data value descriptor as a java.sql.Date.
	 * Throws an exception if the data value is not a Date.
     *	@param cal calendar for object creation
	 * @return	The data value as a java.sql.Date.
	 *
	 * @exception StandardException   Thrown on error
	 */
	Date getDate(java.util.Calendar cal) throws StandardException;

	/**
	 * Gets the value in the data value descriptor as a java.sql.Time.
	 * Throws an exception if the data value is not a Time.
     *	@param cal calendar for object creation
	 *
	 * @return	The data value as a java.sql.Time.
	 *
	 * @exception StandardException   Thrown on error
	 */
	Time	getTime(java.util.Calendar cal) throws StandardException;

	/**
	 * Gets the value in the data value descriptor as a java.sql.Timestamp.
	 * Throws an exception if the data value is not a Timestamp.
     *	@param cal calendar for object creation
	 * @return	The data value as a java.sql.Timestamp.
	 *
	 * @exception StandardException   Thrown on error
	 */
	Timestamp	getTimestamp(java.util.Calendar cal) throws StandardException;

	/**
	 * Gets the value in the data value descriptor as a Java Object.
	 * The type of the Object will be the Java object type corresponding
	 * to the data value's SQL type. JDBC defines a mapping between Java
	 * object types and SQL types - we will allow that to be extended
	 * through user type definitions. Throws an exception if the data
	 * value is not an object (yeah, right).
	 *
	 * @return	The data value as an Object.
	 *
	 * @exception StandardException   Thrown on error
	 */
	Object	getObject() throws StandardException;

    /**
     * Gets the value in the data value descriptor as a stream of bytes.
     * <p>
     * Only data types that implement {@code StreamStorable} will have stream
     * states, and the method {@code hasStream} should be called to determine
     * if the value in question is, or will be, represented by a stream.
     *
     * @return The stream state of the data value.
     * @throws StandardException Throws an exception if the data value
     *      cannot be received as a stream.
     *
     * @see #hasStream()
     * @see StringDataValue#getStreamWithDescriptor()
     */
    InputStream getStream() throws StandardException;

    /**
     * Tells if this data value is, or will be, represented by a stream.
     * <p>
     * This method should be called to determine if the methods {@code
     * getStream} or {@code DataValueDescriptor.getStreamWithDescriptor} can
     * be invoked.
     *
     * @return {@code true} if the value will be a stream, {@code false}
     *      otherwise.
     *
     * @see #getStream()
     * @see StringDataValue#getStreamWithDescriptor()
     */
    boolean hasStream();

    /**
     * Get a shallow copy of this {@code codeDataValueDescriptor} (DVD).
     * <p>
     * The primary use of this method is to avoid materializing streams for
     * data types like BLOB and CLOB.
     * <p>
     * In general the orginal DVD should be recycled or discarded when this
     * method is invoked to ensure that changes to the original DVD don't
     * affect the clone (or the other way around). Note that it is not safe to
     * assume that a number of these clones can be used for read-only access to
     * the same value.
     * <p>
     * <em>Implementation note:</em> The reason why the clones can't be
     * guaranteed to work as "read clones" is that if the value is represented
     * as a stream, the state of the stream will change on read operations.
     * Since all the clones share the same stream, this may lead to wrong
     * results, data corruption or crashes.
     *
     * @return A clone of this descriptor, which shares the internal state.
     */
    public DataValueDescriptor cloneHolder();

    /**
     * Clone this DataValueDescriptor. Results in a new object
     * that has the same value as this but can be modified independently.
     * <p>
     * Even though the objects can be modified independently regardless of the
     * value of {@code forceMaterialization}, both the clone and the
     * original may be dependent on the store state if
     * {@code forceMaterialization} is set to {@code false}. An example is if
     * you need to access the value you just read using {@code cloneValue}
     * after the current transaction has ended, or after the source result set
     * has been closed.
     *
     * @param forceMaterialization any streams representing the data value will
     *      be materialized if {@code true}, the data value will be kept as a
     *      stream if possible if {@code false}
     * @return A clone of the {@code DataValueDescriptor} with the same initial
     *      value as this.
     */
    public abstract DataValueDescriptor cloneValue(
            boolean forceMaterialization);

    /**
     * Recycle this DataValueDescriptor if possible. Create and return a new
     * object if it cannot be recycled.
     *
     * @return this object with the value set to null, or a new
     * DataValueDescriptor of the same type as this
     */
    DataValueDescriptor recycle();

	/**
	 * Get a new null value of the same type as this data value.
	 *
	 */
	public DataValueDescriptor getNewNull();

	/**
	 * Set the value based on the value for the specified DataValueDescriptor
	 * from the specified ResultSet.
	 *
	 * @param resultSet		The specified ResultSet.
	 * @param colNumber		The 1-based column # into the resultSet.
	 * @param isNullable	Whether or not the column is nullable
	 *						(No need to call wasNull() if not)
	 * 
	 * @exception StandardException		Thrown on error
	 * @exception SQLException		Error accessing the result set
	 */
	public void setValueFromResultSet(
    ResultSet   resultSet, 
    int         colNumber,
    boolean     isNullable)
		throws StandardException, SQLException;


	/**
		Set this value into a PreparedStatement. This method must
		handle setting NULL into the PreparedStatement.

		@exception SQLException thrown by the PreparedStatement object
		@exception StandardException thrown by me accessing my value.
	*/
	public void setInto(PreparedStatement ps, int position) throws SQLException, StandardException;

	/**
		Set this value into a ResultSet for a subsequent ResultSet.insertRow
		or ResultSet.updateRow. This method will only be called for non-null values.

		@exception SQLException thrown by the ResultSet object
		@exception StandardException thrown by me accessing my value.
	*/
	public void setInto(ResultSet rs, int position) throws SQLException, StandardException;
	
	/**
	 * Set the value of this DataValueDescriptor to the given int value
	 *
	 * @param theValue	The value to set this DataValueDescriptor to
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setValue(int theValue) throws StandardException;


	/**
	 * Set the value of this DataValueDescriptor to the given double value
	 *
	 * @param theValue	The value to set this DataValueDescriptor to
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setValue(double theValue) throws StandardException;

	/**
	 * Set the value of this DataValueDescriptor to the given double value
	 *
	 * @param theValue	A Double containing the value to set this
	 *					DataValueDescriptor to.  Null means set the value
	 *					to SQL null.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void setValue(float theValue) throws StandardException;

	/**
	 * Set the value of this DataValueDescriptor to the given short value
	 *
	 * @param theValue	The value to set this DataValueDescriptor to
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setValue(short theValue) throws StandardException;

	/**
	 * Set the value of this DataValueDescriptor to the given long value
	 *
	 * @param theValue	The value to set this DataValueDescriptor to
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setValue(long theValue) throws StandardException;

	/**
	 * Set the value of this DataValueDescriptor to the given byte value
	 *
	 * @param theValue	The value to set this DataValueDescriptor to
	 *
	 */
	public void setValue(byte theValue) throws StandardException;

	
	/**
	 * Set the value.
	 *
	 * @param theValue	Contains the boolean value to set this to
	 *
	 */
	public void setValue(boolean theValue) throws StandardException;

	/**
	 * Set the value of this DataValueDescriptor to the given Object value
	 *
	 * @param theValue	The value to set this DataValueDescriptor to
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setValue(Object theValue) throws StandardException;

	/**
	 * Set the value of this DataValueDescriptor.
	 *
	 * @param theValue	The byte value to set this DataValueDescriptor to
	 *
	 */
	public void setValue(byte[] theValue) throws StandardException;

	/**
		Set this value from an application supplied java.math.BigDecimal.
		This is to support the PreparedStatement.setBigDecimal method and
		similar JDBC methods that allow an application to pass in a BigDecimal
		to any SQL type.

		@param bigDecimal required to be a BigDecimal or null.
	 */
	public void setBigDecimal(BigDecimal bigDecimal) throws StandardException;

	/**
	 * Set the value of this DataValueDescriptor.
	 *
	 * @param theValue	The String value to set this DataValueDescriptor to
	 *
	 */
	public void setValue(String theValue) throws StandardException;

 	/**
	 * Set the value of this DataValueDescriptor from a Blob.
	 *
	 * @param theValue	The Blob value to set this DataValueDescriptor to
	 *
	 */
	public void setValue(Blob theValue) throws StandardException;
    
	/**
	 * Set the value of this DataValueDescriptor from a Clob.
	 *
	 * @param theValue	The Clob value to set this DataValueDescriptor to
	 *
	 */
	public void setValue(Clob theValue) throws StandardException;

	/**
	 * Set the value of this DataValueDescriptor.
	 *
	 * @param theValue	The Time value to set this DataValueDescriptor to
	 *
	 */
	public void setValue(Time theValue) throws StandardException;

	/**
	 * Set the value of this DataValueDescriptor.
	 *
	 * @param theValue	The Time value to set this DataValueDescriptor to
     * @param cal The time zone from the calendar is used to construct the database time value
	 *
	 */
	public void setValue(Time theValue, Calendar cal) throws StandardException;

	/**
	 * Set the value of this DataValueDescriptor.
	 *
	 * @param theValue	The Timestamp value to set this DataValueDescriptor to
	 *
	 */
	public void setValue(Timestamp theValue) throws StandardException;

	/**
	 * Set the value of this DataValueDescriptor.
	 *
	 * @param theValue	The Timestamp value to set this DataValueDescriptor to
     * @param cal The time zone from the calendar is used to construct the database timestamp value
	 *
	 */
	public void setValue(Timestamp theValue, Calendar cal) throws StandardException;

	/**
	 * Set the value of this DataValueDescriptor.
	 *
	 * @param theValue	The Date value to set this DataValueDescriptor to
	 *
	 */
	public void setValue(Date theValue) throws StandardException;

	/**
	 * Set the value of this DataValueDescriptor.
	 *
	 * @param theValue	The Date value to set this DataValueDescriptor to
     * @param cal The time zone from the calendar is used to construct the database date value
	 *
	 */
	public void setValue(Date theValue, Calendar cal) throws StandardException;

	/**
	 * Set the value of this DataValueDescriptor from another.
	 *
	 * @param theValue	The Date value to set this DataValueDescriptor to
	 *
	 */
	public void setValue(DataValueDescriptor theValue) throws StandardException;


	/**
	 * Set the value to SQL null.
	 */

	void setToNull();

	/**
		Normalize the source value to this type described by this class
		and the passed in DataTypeDescriptor. The type of the DataTypeDescriptor
		must match this class.
	*/
	public void normalize(DataTypeDescriptor dtd, DataValueDescriptor source)
		throws StandardException;

	/**
	 * The SQL "IS NULL" operator.  Returns true if this value
	 * is null.
	 *	 *
	 * @return	True if this value is null.
	 *
	 */
	public BooleanDataValue isNullOp();

	/**
	 * The SQL "IS NOT NULL" operator.  Returns true if this value
	 * is not null.
	 *
	 *
	 * @return	True if this value is not null.
	 *
	 */
	public BooleanDataValue isNotNull();

	/**
	 * Get the SQL name of the datatype
	 *
	 * @return	The SQL name of the datatype
	 */
	public String	getTypeName();

	/**
	 * Set this value from an Object. Used from CAST of a Java type to
	 * another type, including SQL types. If the passed instanceOfResultType
	 * is false then the object is not an instance of the declared
	 * type resultTypeClassName. Usually an exception should be thrown.
	 *
	 * @param value					The new value
	 * @param instanceOfResultType	Whether or not the new value 
	 *								is an instanceof the result type.
	 * @param resultTypeClassName   The class name of the resulting (declared) type 
     *                              (for error messages only).
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setObjectForCast(
    Object  value, 
    boolean instanceOfResultType, 
    String  resultTypeClassName) 
        throws StandardException;

    /**
     * Read the DataValueDescriptor from the stream.
     * <p>
     * Initialize the data value by reading it's values from the 
     * ArrayInputStream.  This interface is provided as a way to achieve
     * possible performance enhancement when reading an array can be 
     * optimized over reading from a generic stream from readExternal().
     *
     * @param ais    The array stream positioned at the beginning of the 
     *               byte stream to read from.
     *
	 * @exception  IOException              Usual error is if you try to read 
     *                                      past limit on the stream.
	 * @exception  ClassNotFoundException   If a necessary class can not be 
     *                                      found while reading the object from
     *                                      the stream.
     **/
    public void readExternalFromArray(
    ArrayInputStream    ais)
        throws IOException, ClassNotFoundException;

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
	int					typePrecedence();

	/**
	 * The SQL language = operator.  This method is called from the language
	 * module.  The storage module uses the compare method in Orderable.
	 *
	 * @param left		The value on the left side of the operator
	 * @param right		The value on the right side of the operator
	 *
	 * @return	A BooleanDataValue telling the result of the comparison
	 *
	 * @exception StandardException		Thrown on error
	 */
	public BooleanDataValue equals(DataValueDescriptor left,
									DataValueDescriptor right)
						throws StandardException;

	/**
	 * The SQL language &lt;&gt; operator.  This method is called from the language
	 * module.  The storage module uses the compare method in Orderable.
	 *
	 * @param left		The value on the left side of the operator
	 * @param right		The value on the right side of the operator
	 *
	 * @return	A BooleanDataValue telling the result of the comparison
	 *
	 * @exception StandardException		Thrown on error
	 */
	public BooleanDataValue notEquals(DataValueDescriptor left,
									DataValueDescriptor right)
						throws StandardException;

	/**
	 * The SQL language &lt; operator.  This method is called from the language
	 * module.  The storage module uses the compare method in Orderable.
	 *
	 * @param left		The value on the left side of the operator
	 * @param right		The value on the right side of the operator
	 *
	 * @return	A BooleanDataValue telling the result of the comparison
	 *
	 * @exception StandardException		Thrown on error
	 */
	public BooleanDataValue lessThan(DataValueDescriptor left,
									DataValueDescriptor right)
						throws StandardException;

	/**
	 * The SQL language &gt; operator.  This method is called from the language
	 * module.  The storage module uses the compare method in Orderable.
	 *
	 * @param left		The value on the left side of the operator
	 * @param right		The value on the right side of the operator
	 *
	 * @return	A BooleanDataValue telling the result of the comparison
	 *
	 * @exception StandardException		Thrown on error
	 */
	public BooleanDataValue greaterThan(DataValueDescriptor left,
									DataValueDescriptor right)
						throws StandardException;

	/**
	 * The SQL language &lt;= operator.  This method is called from the language
	 * module.  The storage module uses the compare method in Orderable.
	 *
	 * @param left		The value on the left side of the operator
	 * @param right		The value on the right side of the operator
	 *
	 * @return	A BooleanDataValue telling the result of the comparison
	 *
	 * @exception StandardException		Thrown on error
	 */
	public BooleanDataValue lessOrEquals(DataValueDescriptor left,
									DataValueDescriptor right)
						throws StandardException;

	/**
	 * The SQL language &gt;= operator.  This method is called from the language
	 * module.  The storage module uses the compare method in Orderable.
	 *
	 * @param left		The value on the left side of the operator
	 * @param right		The value on the right side of the operator
	 *
	 * @return	A BooleanDataValue telling the result of the comparison
	 *
	 * @exception StandardException		Thrown on error
	 */
	public BooleanDataValue greaterOrEquals(DataValueDescriptor left,
									DataValueDescriptor right)
						throws StandardException;


	/**
	 * The SQL language COALESCE/VALUE function.  This method is called from the language
	 * module.  
	 *
	 * @param list		The list of the arguments. Function will return the first non-nullable argument if any.
	 * @param returnValue		The return value is the correct datatype for this function.
	 * The return value of this method is the type of the 2nd parameter.
	 *
	 * @return	A DataValueDescriptor which will be either null or first non-null argument
	 *
	 * @exception StandardException		Thrown on error
	 */
	public DataValueDescriptor coalesce(DataValueDescriptor[] list, DataValueDescriptor returnValue)
						throws StandardException;

	/**
	 * The SQL language IN operator.  This method is called from the language
	 * module.  This method allows us to optimize and short circuit the search
	 * if the list is ordered.
	 *
	 * @param left		The value on the left side of the operator
	 * @param inList	The values in the IN list
	 * @param orderedList	True means that the values in the IN list are ordered,
	 *						false means they are not.
	 *
	 * @return	A BooleanDataValue telling the result of the comparison
	 *
	 * @exception StandardException		Thrown on error
	 */
	public BooleanDataValue in(DataValueDescriptor left,
							   DataValueDescriptor[] inList,
							   boolean orderedList) 
						throws StandardException;

	/**
	 * Compare this Orderable with a given Orderable for the purpose of
	 * index positioning.  This method treats nulls as ordered values -
	 * that is, it treats SQL null as equal to null and greater than all
	 * other values.
	 *
	 * @param other		The Orderable to compare this one to.
	 *
	 * @return  &lt;0 - this Orderable is less than other.
	 * 			 0 - this Orderable equals other.
	 *			&gt;0 - this Orderable is greater than other.
     *
     *			The code should not explicitly look for -1, or 1.
	 *
	 * @exception StandardException		Thrown on error
	 */
	int compare(DataValueDescriptor other) throws StandardException;

	/**
	 * Compare this Orderable with another, with configurable null ordering.
	 * This method treats nulls as ordered values, but allows the caller
         * to specify whether they should be lower than all non-NULL values,
         * or higher than all non-NULL values.
	 *
	 * @param other		The Orderable to compare this one to.
         % @param nullsOrderedLow True if null should be lower than non-NULL
	 *
	 * @return  &lt;0 - this Orderable is less than other.
	 * 			 0 - this Orderable equals other.
	 *			&gt;0 - this Orderable is greater than other.
     *
     *			The code should not explicitly look for -1, or 1.
	 *
	 * @exception StandardException		Thrown on error
	 */
	int compare(DataValueDescriptor other, boolean nullsOrderedLow)
            throws StandardException;

	/**
	 * Compare this Orderable with a given Orderable for the purpose of
	 * qualification and sorting.  The caller gets to determine how nulls
	 * should be treated - they can either be ordered values or unknown
	 * values.
	 *
	 * @param op	Orderable.ORDER_OP_EQUALS means do an = comparison.
	 *				Orderable.ORDER_OP_LESSTHAN means compare this &lt; other.
	 *				Orderable.ORDER_OP_LESSOREQUALS means compare this &lt;= other.
	 * @param other	The DataValueDescriptor to compare this one to.
	 * @param orderedNulls	True means to treat nulls as ordered values,
	 *						that is, treat SQL null as equal to null, and less
	 *						than all other values.
	 *						False means to treat nulls as unknown values,
	 *						that is, the result of any comparison with a null
	 *						is the UNKNOWN truth value.
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
	boolean compare(
    int         op, 
    DataValueDescriptor   other,
    boolean     orderedNulls, 
    boolean     unknownRV)
				throws StandardException;

	/**
	 * Compare this Orderable with another, with configurable null ordering.
	 * The caller gets to determine how nulls
	 * should be treated - they can either be ordered values or unknown
	 * values. The caller also gets to decide, if they are ordered,
         * whether they should be lower than non-NULL values, or higher
	 *
	 * @param op	Orderable.ORDER_OP_EQUALS means do an = comparison.
	 *				Orderable.ORDER_OP_LESSTHAN means compare this &lt; other.
	 *				Orderable.ORDER_OP_LESSOREQUALS means compare this &lt;= other.
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
	boolean compare(
    int         op, 
    DataValueDescriptor   other,
    boolean     orderedNulls, 
    boolean     nullsOrderedLow,
    boolean     unknownRV)
				throws StandardException;

    /**
     * Set the value to be the contents of the stream.
     * <p>
     * The reading of the stream may be delayed until execution time, and the
     * format of the stream is required to be the format of this type.
     * <p>
     * Note that the logical length excludes any header bytes and marker bytes
     * (for instance the Derby specific EOF stream marker). Specifying the
     * logical length may improve performance in some cases, but specifying
     * that the length is unknown (<code>UNKNOWN_LOGICAL_LENGTH</code> should
     * always leave the system in a functional state. Specifying an incorrect
     * length will cause errors.
     *
     * @param theStream	stream of correctly formatted data
     * @param valueLength logical length of the stream's value in units of this
     *      type (e.g. chars for string types), or
     *      <code>UNKNOWN_LOGICAL_LENGTH</code> if the logical length is unknown
     */
	public void setValue(InputStream theStream, int valueLength) throws StandardException;

	/**
		Check the value to seem if it conforms to the restrictions
		imposed by DB2/JCC on host variables for this type.

		@exception StandardException Variable is too big.
	*/
	public void checkHostVariable(int declaredLength) throws StandardException;

    /**
     * Estimate the memory usage in bytes of the data value and the overhead of the class.
     *
     * @return the estimated memory usage
     */
    int estimateMemoryUsage();
}
