/*

   Derby - Class org.apache.derby.iapi.types.StringDataValue

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

import java.sql.Clob;

import org.apache.derby.shared.common.error.StandardException;

import java.text.RuleBasedCollator;
import org.apache.derby.iapi.jdbc.CharacterStreamDescriptor;

public interface StringDataValue extends ConcatableDataValue
{
	// TRIM() types
	public static final int BOTH		= 0;
	public static final int TRAILING	= 1;
	public static final int LEADING		= 2;

	/**
	  For a character string type, the collation derivation should always be 
	  "explicit"(not possible in Derby 10.3), "implicit" or "none". We will 
//IC see: https://issues.apache.org/jira/browse/DERBY-2599
	  start by setting it to "implicit" in TypeDescriptorImpl. At runtime, only 
	  character string types which are results of aggregate methods dealing 
	  with operands with different collation types should have a collation 
	  derivation of "none". All the other character string types should have 
	  their collation derivation set to "implicit". 
	 */
	public	static final int COLLATION_DERIVATION_NONE = 0;
	/** @see StringDataValue#COLLATION_DERIVATION_NONE */
	public	static final int COLLATION_DERIVATION_IMPLICIT = 1;
	/** @see StringDataValue#COLLATION_DERIVATION_NONE */
	public	static final int COLLATION_DERIVATION_EXPLICIT = 2;
	/**
	 * In Derby 10.3, it is possible to have database with one of the following
	 * two configurations
	 * 1)all the character columns will have a collation type of UCS_BASIC. 
	 * This is same as what we do in Derby 10.2 release. 
	 * 2)all the character string columns belonging to system tables will have 
	 * collation type of UCS_BASIC but all the character string columns 
	 * belonging to user tables will have collation type of TERRITORY_BASED.
	 * 
	 * Data types will start with collation type defaulting to UCS_BASIC in
	 * TypeDescriptorImpl. This collation type of course makes sense for
	 * character string types only. It will be ignored for the rest of the
	 * types. If a character's collation type should be TERRITORY_BASED, then
	 * DTD.setCollationType can be called to change the default of UCS_BASIC.
	 *
	 * The collation types TERRITORY_BASED:PRIMARY through TERRITORY_BASED:IDENTICAL
	 * are variants of the TERRITORY_BASED type, with explicit collation strength.
	 */
	public	static final int COLLATION_TYPE_UCS_BASIC = 0;
	/** @see StringDataValue#COLLATION_TYPE_UCS_BASIC */
	public	static final int COLLATION_TYPE_TERRITORY_BASED = 1;
	public	static final int COLLATION_TYPE_TERRITORY_BASED_PRIMARY = 2;
	public	static final int COLLATION_TYPE_TERRITORY_BASED_SECONDARY = 3;
	public	static final int COLLATION_TYPE_TERRITORY_BASED_TERTIARY = 4;
	public	static final int COLLATION_TYPE_TERRITORY_BASED_IDENTICAL = 5;

	/**
	 * The SQL concatenation '||' operator.
	 *
	 * @param leftOperand	String on the left hand side of '||'
	 * @param rightOperand	String on the right hand side of '||'
	 * @param result	The result of a previous call to this method,
	 *					null if not called yet.
	 *
	 * @return	A ConcatableDataValue containing the result of the '||'
	 *
	 * @exception StandardException		Thrown on error
	 */
	public StringDataValue concatenate(
				StringDataValue leftOperand,
				StringDataValue rightOperand,
				StringDataValue result)
		throws StandardException;

	/**
	 * The SQL like() function with out escape clause.
	 *
	 * @param pattern	the pattern to use
	 *
	 * @return	A BooleanDataValue containing the result of the like
	 *
	 * @exception StandardException		Thrown on error
	 */
	public BooleanDataValue like(DataValueDescriptor pattern)
							throws StandardException;

	/**
	 * The SQL like() function WITH escape clause.
	 *
	 * @param pattern	the pattern to use
	 * @param escape	the escape character
	 *
	 * @return	A BooleanDataValue containing the result of the like
	 *
	 * @exception StandardException		Thrown on error
	 */
	public BooleanDataValue like(DataValueDescriptor pattern,
									DataValueDescriptor escape)
							throws StandardException;


	/**
	 * The SQL Ansi trim function.

	 * @param trimType type of trim. Possible values are {@link #LEADING}, {@link #TRAILING}
	 *        or {@link #BOTH}.
	 * @param trimChar  The character to trim from <em>this</em>
	 * @param result The result of a previous call to this method,
	 *					null if not called yet.
	 * @return A StringDataValue containing the result of the trim().
	 * @throws StandardException
	 */
	public StringDataValue ansiTrim(
			int trimType,
			StringDataValue trimChar,
			StringDataValue result)
		throws StandardException;

	/** 
	 * Convert the string to upper case.
	 *
	 * @param result	The result (reusable - allocate if null).
	 * 
	 * @return	The string converted to upper case.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public StringDataValue upper(StringDataValue result)
							throws StandardException;

	/** 
	 * Convert the string to lower case.
	 *
	 * @param result	The result (reusable - allocate if null).
	 * 
	 * @return	The string converted to lower case.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public StringDataValue lower(StringDataValue result)
							throws StandardException;

    /**
     * Position in searchFrom of the first occurrence of this.value.
     * The search begins from position start.  0 is returned if searchFrom does
     * not contain this.value.  Position 1 is the first character in searchFrom.
     *
     * @param searchFrom    - The string to search from
     * @param start         - The position to search from in string searchFrom
     * @param result        - The object to return
     *
     * @return  The position in searchFrom the fist occurrence of this.value.
     *              0 is returned if searchFrom does not contain this.value.
     * @exception StandardException     Thrown on error
     */
    public NumberDataValue locate(  StringDataValue searchFrom, 
                                    NumberDataValue start,
                                    NumberDataValue result)
                                    throws StandardException;


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
	public char[] getCharArray() throws StandardException;

	/**
	 * Gets either SQLChar/SQLVarchar/SQLLongvarchar/SQLClob(base classes) or 
	 * CollatorSQLChar/CollatorSQLVarchar/CollatorSQLLongvarch/CollatorSQLClob
	 * (subclasses). Whether this method returns the base class or the subclass 
	 * depends on the value of the RuleBasedCollator. If RuleBasedCollator is 
	 * null, then the object returned would be baseclass otherwise it would be 
	 * subcalss.
	 */
	public StringDataValue getValue(RuleBasedCollator collatorForComparison);

    /**
     * Returns the stream header generator for the string data value.
     * <p>
     * The generator writes the correct header into the destination buffer or
     * stream and also keeps track of whether appending an end-of-stream marker
     * is required or not.
     * <p>
     * Note that the generator may fail to generate a header if there is no
     * context at the time the header is asked for, and the mode hasn't been
     * set explicitly.
     * @see #setStreamHeaderFormat
     */
    public StreamHeaderGenerator getStreamHeaderGenerator();

    /**
     * Tells the data value descriptor which CLOB stream header format to use.
     *
     * @param usePreTenFiveHdrFormat {@code true} if the database accessed is
     *      prior to version 10.5, {@code false} if the version is 10.5 or
     *      newer, and {@code null} if unknown at this time
     */
    public void setStreamHeaderFormat(Boolean usePreTenFiveHdrFormat);

    /**
     * Returns a descriptor for the input stream for this data value.
     * <p>
     * The descriptor contains information about header data, current positions,
     * length, whether the stream should be buffered or not, and if the stream
     * is capable of repositioning itself.
     *
     * @return A descriptor for the stream, which includes a reference to the
     *      stream itself.
     * @throws StandardException if obtaining the descriptor fails, or if the
     *      value isn't represented as a stream.
     */
    public CharacterStreamDescriptor getStreamWithDescriptor()
            throws StandardException;


	/**
	 * Stuff a StringDataValue with a Clob.
	 */
	public void setValue( Clob value )
//IC see: https://issues.apache.org/jira/browse/DERBY-3907
		throws StandardException;
}
