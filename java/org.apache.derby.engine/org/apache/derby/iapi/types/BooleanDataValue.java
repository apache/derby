/*

   Derby - Class org.apache.derby.iapi.types.BooleanDataValue

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

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.sql.Activation;

public interface BooleanDataValue extends DataValueDescriptor
{
	public boolean	getBoolean();

	/**
	 * The SQL AND operator.  This provides SQL semantics for AND with unknown
	 * truth values - consult any standard SQL reference for an explanation.
	 *
	 * @param otherValue	The other BooleanDataValue to AND with this one
	 *
	 * @return	this AND otherValue
	 *
	 */
	public BooleanDataValue and(BooleanDataValue otherValue);

	/**
	 * The SQL OR operator.  This provides SQL semantics for OR with unknown
	 * truth values - consult any standard SQL reference for an explanation.
	 *
	 * @param otherValue	The other BooleanDataValue to OR with this one
	 *
	 * @return	this OR otherValue
	 *
	 */
	public BooleanDataValue or(BooleanDataValue otherValue);

	/**
	 * The SQL IS operator - consult any standard SQL reference for an explanation.
	 *
	 *	Implements the following truth table:
	 *
	 *	         otherValue
	 *	        | TRUE    | FALSE   | UNKNOWN
	 *	this    |----------------------------
	 *	        |
	 *	TRUE    | TRUE    | FALSE   | FALSE
	 *	FALSE   | FALSE   | TRUE    | FALSE
	 *	UNKNOWN | FALSE   | FALSE   | TRUE
	 *
	 *
	 * @param otherValue	BooleanDataValue to compare to. May be TRUE, FALSE, or UNKNOWN.
	 *
	 * @return	whether this IS otherValue
	 *
	 */
	public BooleanDataValue is(BooleanDataValue otherValue);

	/**
	 * Implements NOT IS. This reverses the sense of the is() call.
	 *
	 *
	 * @param otherValue	BooleanDataValue to compare to. May be TRUE, FALSE, or UNKNOWN.
	 *
	 * @return	NOT( this IS otherValue )
	 *
	 */
	public BooleanDataValue isNot(BooleanDataValue otherValue);

	/**
	 * Throw an exception with the given SQLState if this BooleanDataValue
	 * is false. This method is useful for evaluating constraints.
	 *
	 * @param SQLState		The SQLState of the exception to throw if
	 *						this SQLBoolean is false.
	 * @param tableName		The name of the table to include in the exception
	 *						message.
	 * @param constraintName	The name of the failed constraint to include
	 *							in the exception message.
	 *
	 * @return	this
	 *
	 * @exception	StandardException	Thrown if this BooleanDataValue
	 *									is false.
	 */
	public BooleanDataValue throwExceptionIfFalse(
									String SQLState,
									String tableName,
									String constraintName)
							throws StandardException;

    /**
     * If this value is false and we have a deferred constraint, remember the
     * violation and proceed, else throw.  See also
     * org.apache.derby.impl.sql.compile.AndNoShortCircuitNode.
     *
     * @param SQLState      The SQLState of the exception to throw if
     *                      this SQLBoolean is false.
     * @param tableName     The name of the table to include in the exception
     *                      message.
     * @param constraintName    The name of the failed constraint to include
     *                          in the exception message.
     * @param a             The activation
     * @param savedUUIDIdx  The saved object number of the constraint's UUID.
     *
     * @return  this
     *
     * @exception   StandardException   Thrown if this BooleanDataValue
     *                                  is false.
     */
    public BooleanDataValue throwExceptionIfImmediateAndFalse(
                                    String SQLState,
                                    String tableName,
                                    String constraintName,
                                    Activation a,
                                    int savedUUIDIdx)
                            throws StandardException;

    /*
	** NOTE: The NOT operator is translated to "= FALSE", which does the same
	** thing.
	*/

	/**
	 * Set the value of this BooleanDataValue.
	 *
	 * @param theValue	Contains the boolean value to set this BooleanDataValue
	 *					to.  Null means set this BooleanDataValue to null.
	 */
	public void setValue(Boolean theValue);

	/**
	 * Tell whether a BooleanDataValue has the given value.  This is useful
	 * for short-circuiting.
	 *
	 * @param value		The value to look for
	 *
	 * @return	true if the BooleanDataValue contains the given value.
	 */
	public boolean equals(boolean value);
	
	/**
	 * Return an immutable BooleanDataValue with the same value as this.
	 * @return An immutable BooleanDataValue with the same value as this.
	 */
	public BooleanDataValue getImmutable();
}
