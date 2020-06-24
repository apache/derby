/*

   Derby - Class org.apache.derby.iapi.types.SQLBoolean

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

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.iapi.services.cache.ClassSize;
import org.apache.derby.iapi.util.StringUtil;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.math.BigDecimal;

import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.Row;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.impl.sql.execute.DMLWriteResultSet;

/**
 * SQLBoolean satisfies the DataValueDescriptor
 * interfaces (i.e., DataType). It implements a boolean column, 
 * e.g. for * storing a column value; it can be specified
 * when constructed to not allow nulls. Nullability cannot be changed
 * after construction, as it affects the storage size and mechanism.
 * <p>
 * Because DataType is a subtype of DataType,
 * SQLBoolean can play a role in either a DataType/Row
 * or a DataType/Row, interchangeably.
 * <p>
 * We assume the store has a flag for nullness of the value,
 * and simply return a 0-length array for the stored form
 * when the value is null.
 * <p>
 * PERFORMANCE: There are likely alot of performance improvements
 * possible for this implementation -- it new's Integer
 * more than it probably wants to.
 */
public final class SQLBoolean
	extends DataType implements BooleanDataValue
{
	/*
	 * DataValueDescriptor interface
	 * (mostly implemented in DataType)
	 */

	/*
	 * see if the integer value is null.
	 */
	public boolean isNull()
	{
		return isnull;
	}

	public boolean	getBoolean()
	{
		return value;
	}

	private static int makeInt(boolean b)
	{
		return (b?1:0);
	}

	/** 
	 * @see DataValueDescriptor#getByte 
	 */
	public byte	getByte() 
	{
		return (byte) makeInt(value);
	}

	/** 
	 * @see DataValueDescriptor#getShort 
	 */
	public short	getShort()
	{
		return (short) makeInt(value);
	}

	/** 
	 * @see DataValueDescriptor#getInt 
	 */
	public int	getInt()
	{
		return makeInt(value);
	}

	/** 
	 * @see DataValueDescriptor#getLong 
	 */
	public long	getLong()
	{
		return (long) makeInt(value);
	}

	/** 
	 * @see DataValueDescriptor#getFloat 
	 */
	public float	getFloat()
	{
		return (float) makeInt(value);
	}

	/** 
	 * @see DataValueDescriptor#getDouble 
	 */
	public double	getDouble()
	{
		return (double) makeInt(value);
	}

	/**
	 * Implementation for BOOLEAN type. Convert to a BigDecimal using long
	 */
	public int typeToBigDecimal()
	{
		return java.sql.Types.BIGINT;
	}
	public String	getString()
	{
		if (isNull())
			return null;
		else if (value == true)
			return "true";
		else
			return "false";
	}

	public Object	getObject()
	{
		if (isNull())
			return null;
		else
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
			return value;
	}

	public int	getLength()
	{
		return BOOLEAN_LENGTH;
	}

	// this is for DataType's error generator
	public String getTypeName()
	{
		return TypeId.BOOLEAN_NAME;
	}

    /**
     * Recycle this SQLBoolean object if possible. If the object is immutable,
     * create and return a new object.
     *
     * @return a new SQLBoolean if this object is immutable; otherwise, this
     * object with value set to null
     */
    public DataValueDescriptor recycle() {
        if (immutable) {
            return new SQLBoolean();
        }
        return super.recycle();
    }

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */


	/**
		Return my format identifier.

		@see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.SQL_BOOLEAN_ID;
	}

	public void writeExternal(ObjectOutput out) throws IOException {

		// never called when value is null
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(! isNull());

		out.writeBoolean(value);
	}

	/** @see java.io.Externalizable#readExternal */
	public void readExternal(ObjectInput in) throws IOException {

		if (SanityManager.DEBUG)
			SanityManager.ASSERT( ! immutable,
						"Attempt to set the value of an immutable SQLBoolean");

		value = in.readBoolean();
		isnull = false;
	}

	/**
	 * @see Storable#restoreToNull
	 *
	 */
	public void restoreToNull()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT( ! immutable,
						"Attempt to set the value of an immutable SQLBoolean");

		value = false;
		isnull = true;
	}

	/*
	 * Orderable interface
	 */

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

		boolean thisNull, otherNull;
		thisNull = this.isNull();
		otherNull = other.isNull();

		/*
		 * thisNull otherNull thisValue thatValue return
		 *	T		T			X		X			0	(this == other)
		 *	F		T			X		X			-1 	(this > other)
		 *	T		F			X		X			1	(this < other)
		 *
		 *	F		F			T		T			0	(this == other)
		 *	F		F			T		F			1	(this > other)
		 *	F		F			F		T			-1	(this < other)
		 *	F		F			F		F			0	(this == other)
		 */
		if (thisNull || otherNull)
		{
			if (!thisNull)		// otherNull must be true
				return -1;
//IC see: https://issues.apache.org/jira/browse/DERBY-4716
			if (!otherNull)		// thisNull must be true
				return 1;
			return 0;
		}

		/* neither are null, get the value */
		boolean thisValue;
		boolean otherValue = false;
		thisValue = this.getBoolean();

		otherValue = other.getBoolean();

		if (thisValue == otherValue)
			return 0;
		else if (thisValue && !otherValue)
			return 1;
		else
			return -1;
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
			if (this.isNull() || other.isNull())
				return unknownRV;
		}
		/* Do the comparison */
		return super.compare(op, other, orderedNulls, unknownRV);
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#cloneValue */
	public DataValueDescriptor cloneValue(boolean forceMaterialization)
	{
		return new SQLBoolean(value, isnull);
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLBoolean();
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
			value = resultSet.getBoolean(colNumber);
			isnull = (isNullable && resultSet.wasNull());
	}
	/**
		Set the value into a PreparedStatement.

		@exception SQLException Error setting value in PreparedStatement
	*/
	public final void setInto(PreparedStatement ps, int position) throws SQLException {

		if (isNull()) {
			ps.setNull(position, java.sql.Types.BIT);
			return;
		}

		ps.setBoolean(position, value);
	}
	/*
	 * class interface
	 */

	/*
	 * constructors
	 */

	/* NOTE - other data types have both (type value) and (boolean nulls), 
	 * (value, nulls)
	 * We can't do both (boolean value) and (boolean nulls) here,
	 * so we'll skip over (boolean value) and have (Boolean value) so
	 * that we can support (boolean nulls).
	 */

	public SQLBoolean()
	{
		isnull = true;
	}

	public SQLBoolean(boolean val)
	{
		value = val;
	}
	public SQLBoolean(Boolean obj) {
		if (isnull = (obj == null))
			;
		else
			value = obj.booleanValue();
	}

    /* This constructor gets used for the cloneValue method */
	private SQLBoolean(boolean val, boolean isnull)
	{
		value = val;
		this.isnull = isnull;
	}

	/** @see BooleanDataValue#setValue */
	public void setValue(boolean theValue)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT( ! immutable,
						"Attempt to set the value of an immutable SQLBoolean");
		value = theValue;
		isnull = false;

	}

	public void setValue(Boolean theValue)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT( ! immutable,
						"Attempt to set the value of an immutable SQLBoolean");
		if (theValue == null)
		{
			value = false;
			isnull = true;
		}
		else
		{
			value = theValue.booleanValue();
			isnull = false;
		}

	}

	// REMIND: do we need this, or is long enough?
	public void setValue(byte theValue)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT( ! immutable,
						"Attempt to set the value of an immutable SQLBoolean");
		value = theValue != 0;
		isnull = false;

	}


	// REMIND: do we need this, or is long enough?
	public void setValue(short theValue)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT( ! immutable,
						"Attempt to set the value of an immutable SQLBoolean");
		value = theValue != 0;
		isnull = false;

	}


	// REMIND: do we need this, or is long enough?
	public void setValue(int theValue)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT( ! immutable,
						"Attempt to set the value of an immutable SQLBoolean");
		value = theValue != 0;
		isnull = false;

	}

	public void setValue(long theValue)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT( ! immutable,
						"Attempt to set the value of an immutable SQLBoolean");
		value = theValue != 0;
		isnull = false;

	}

	// REMIND: do we need this, or is double enough?
	public void setValue(float theValue)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT( ! immutable,
						"Attempt to set the value of an immutable SQLBoolean");
		value = theValue != 0;
		isnull = false;

	}

	public void setValue(double theValue)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT( ! immutable,
						"Attempt to set the value of an immutable SQLBoolean");
		value = theValue != 0;
		isnull = false;

	}

	public void setBigDecimal(BigDecimal bigDecimal) throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT( ! immutable,
						"Attempt to set the value of an immutable SQLBoolean");
		if (bigDecimal == null)
		{
			value = false;
			isnull = true;
		}
		else
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
            value = BigDecimal.ZERO.compareTo(bigDecimal) != 0;
			isnull = false;
		}

	}

	/**
	 * Set the value of this BooleanDataValue to the given String.
	 * String is trimmed and upcased.  If resultant string is not
	 * TRUE or FALSE, then an error is thrown.
	 *
	 * @param theValue	The value to set this BooleanDataValue to
	 *
	 * @exception StandardException Thrown on error
	 */
	public void setValue(String theValue)
		throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT( ! immutable,
						"Attempt to set the value of an immutable SQLBoolean");
		if (theValue == null)
		{
			value = false;
			isnull = true;
		}
		else
		{
			/*
			** Note: cannot use getBoolean(String) here because
			** it doesn't trim, and doesn't throw exceptions.
			*/
			String cleanedValue = StringUtil.SQLToUpperCase(theValue.trim());
			if (cleanedValue.equals("TRUE"))
			{
				value = true;
//IC see: https://issues.apache.org/jira/browse/DERBY-4658
                isnull = false;
			}
			else if (cleanedValue.equals("FALSE"))
			{
				value = false;
                isnull = false;
			}
			else if (cleanedValue.equals("UNKNOWN"))
			{
				value = false;
                isnull = true;
			}
			else
			{ 
				throw invalidFormat();
			}
		}

	}

	/**
	 * @see DataValueDescriptor#setValue
	 */	
//IC see: https://issues.apache.org/jira/browse/DERBY-776
	void setObject(Object theValue)
	{
		setValue((Boolean) theValue);
	}
	protected void setFrom(DataValueDescriptor theValue) throws StandardException {

//IC see: https://issues.apache.org/jira/browse/DERBY-4684
        if ( theValue instanceof SQLChar ) { setValue( theValue.getString() ); }
		else if ( theValue instanceof SQLBoolean ){ setValue(theValue.getBoolean()); }
        else
        {
            throw StandardException.newException
                ( SQLState.LANG_DATA_TYPE_SET_MISMATCH, theValue.getTypeName(), getTypeName() );

        }
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
		return truthValue(left,
							right,
							left.getBoolean() == right.getBoolean());
	}

	/**
	 * The &lt;&gt; operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the operator
	 * @param right			The value on the right side of the operator
	 *
	 * @return	A SQL boolean value telling whether the two parameters are
	 *			not equal
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue notEquals(DataValueDescriptor left,
							 DataValueDescriptor right)
				throws StandardException
	{
		return truthValue(left,
							right,
							left.getBoolean() != right.getBoolean());
	}

	/**
	 * The &lt; operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the operator
	 * @param right			The value on the right side of the operator
	 *
	 * @return	A SQL boolean value telling whether the left operand is
	 *			less than the right operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue lessThan(DataValueDescriptor left,
							 DataValueDescriptor right)
				throws StandardException
	{
		/* We must call getBoolean() on both sides in order
		 * to catch any invalid casts.
		 */
		boolean leftBoolean = left.getBoolean();
		boolean rightBoolean = right.getBoolean();
		/* By convention, false is less than true */
		return truthValue(left,
							right,
							leftBoolean == false && rightBoolean == true);
	}

	/**
	 * The &gt; operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the operator
	 * @param right			The value on the right side of the operator
	 *
	 * @return	A SQL boolean value telling whether the left operand is
	 *			greater than the right operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue greaterThan(DataValueDescriptor left,
							 DataValueDescriptor right)
				throws StandardException
	{
		/* We must call getBoolean() on both sides in order
		 * to catch any invalid casts.
		 */
		boolean leftBoolean = left.getBoolean();
		boolean rightBoolean = right.getBoolean();
		/* By convention, true is greater than false */
		return truthValue(left,
							right,
							leftBoolean == true && rightBoolean == false);
	}

	/**
	 * The &lt;= operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the operator
	 * @param right			The value on the right side of the operator
	 *
	 * @return	A SQL boolean value telling whether the left operand is
	 *			less than or equal to the right operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue lessOrEquals(DataValueDescriptor left,
							 DataValueDescriptor right)
				throws StandardException
	{
		/* We must call getBoolean() on both sides in order
		 * to catch any invalid casts.
		 */
		boolean leftBoolean = left.getBoolean();
		boolean rightBoolean = right.getBoolean();
		/* By convention, false is less than true */
		return truthValue(left,
							right,
							leftBoolean == false || rightBoolean == true);
	}

	/**
	 * The &gt;= operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the operator
	 * @param right			The value on the right side of the operator
	 *
	 * @return	A SQL boolean value telling whether the left operand is
	 *			greater than or equal to the right operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue greaterOrEquals(DataValueDescriptor left,
							 DataValueDescriptor right)
				throws StandardException
	{
		/* We must call getBoolean() on both sides in order
		 * to catch any invalid casts.
		 */
		boolean leftBoolean = left.getBoolean();
		boolean rightBoolean = right.getBoolean();
		/* By convention, true is greater than false */
		return truthValue(left,
							right,
							leftBoolean == true || rightBoolean == false);
	}

	/**
	 * The AND operator.  This implements SQL semantics for AND with unknown
	 * truth values - consult any standard SQL reference for an explanation.
	 *
	 * @param otherValue	The other boolean to AND with this one
	 *
	 * @return	this AND otherValue
	 *
	 */

	public BooleanDataValue and(BooleanDataValue otherValue)
	{
		/*
		** Catch those cases where standard SQL null semantics don't work.
		*/
		if (this.equals(false) || otherValue.equals(false))
		{
			return BOOLEAN_FALSE;
		}
		else
		{
			return truthValue(this,
							otherValue,
							this.getBoolean() && otherValue.getBoolean());
		}
	}

	/**
	 * The OR operator.  This implements SQL semantics for OR with unknown
	 * truth values - consult any standard SQL reference for an explanation.
	 *
	 * @param otherValue	The other boolean to OR with this one
	 *
	 * @return	this OR otherValue
	 *
	 */

	public BooleanDataValue or(BooleanDataValue otherValue)
	{
		/*
		** Catch those cases where standard SQL null semantics don't work.
		*/
		if (this.equals(true) || otherValue.equals(true))
		{
			return BOOLEAN_TRUE;
		}
		else
		{
			return truthValue(this,
							otherValue,
							this.getBoolean() || otherValue.getBoolean());
		}
	}

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
	public BooleanDataValue is(BooleanDataValue otherValue)
	{
		if ( this.equals(true) && otherValue.equals(true) )
		{ return BOOLEAN_TRUE; }

		if ( this.equals(false) && otherValue.equals(false) )
		{ return BOOLEAN_TRUE; }

		if ( this.isNull() && otherValue.isNull() )
		{ return BOOLEAN_TRUE; }

		return BOOLEAN_FALSE;
	}

	/**
	 * Implements NOT IS. This reverses the sense of the is() call.
	 *
	 *
	 * @param otherValue	BooleanDataValue to compare to. May be TRUE, FALSE, or UNKNOWN.
	 *
	 * @return	NOT( this IS otherValue )
	 *
	 */
	public BooleanDataValue isNot(BooleanDataValue otherValue)
	{
		BooleanDataValue	isValue = is( otherValue );

		if ( isValue.equals(true) ) { return BOOLEAN_FALSE; }
		else { return BOOLEAN_TRUE; }
	}

	/**
	 * Throw an exception with the given SQLState if this BooleanDataValue
	 * is false. This method is useful for evaluating constraints.
	 *
	 * @param sqlState		The SQLState of the exception to throw if
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
									String sqlState,
									String tableName,
									String constraintName)
							throws StandardException
	{
		if ( ( ! isNull() ) && (value == false) )
		{
			throw StandardException.newException(sqlState,
												tableName,
												constraintName);
		}

		return this;
	}

    public BooleanDataValue throwExceptionIfImmediateAndFalse(
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                                    String sqlState,
                                    String tableName,
                                    String constraintName,
                                    Activation a,
                                    int savedUUIDIdx)
                            throws StandardException
    {
        if ( !isNull() && (value == false) ) {
            final ExecPreparedStatement ps = a.getPreparedStatement();
            final UUID constrId = (UUID)ps.getSavedObject(savedUUIDIdx);
            final LanguageConnectionContext lcc =
                a.getLanguageConnectionContext();
//IC see: https://issues.apache.org/jira/browse/DERBY-532
            final boolean isDeferred = lcc.isEffectivelyDeferred(
                   lcc.getCurrentSQLSessionContext(a), constrId);

            if (!isDeferred) {
                throw StandardException.newException(
                        sqlState, tableName, constraintName);
            } else {
                // Just return the false value and validate later,
                // cf NoRowsResultSetImpl#evaluateCheckConstraints.
                // and InsertResultSet#evaluateCheckConstraints
                DMLWriteResultSet rs =  (DMLWriteResultSet)a.getResultSet();
                rs.rememberConstraint(constrId);
            }
        }

        return this;
    }
	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.BOOLEAN_PRECEDENCE;
	}

	/*
	** Support functions
	*/

	/**
	 * Return the SQL truth value for a comparison.
	 *
	 * This method first looks at the operands - if either is null, it
	 * returns the unknown truth value.  This implements "normal" SQL
	 * null semantics, where if any operand is null, the result is null.
	 * Note that there are cases where these semantics are incorrect -
	 * for example, NULL AND FALSE is supposed to be FALSE, not NULL
	 * (the NULL truth value is the same as the UNKNOWN truth value).
	 *
	 * If neither operand is null, it returns a static final variable
	 * containing the SQLBoolean truth value.  It returns different values
	 * depending on whether the truth value is supposed to be nullable.
	 *
	 * This method always returns a pre-allocated static final SQLBoolean.
	 * This is practical because there are so few possible return values.
	 * Using pre-allocated values allows us to avoid constructing new
	 * SQLBoolean values during execution.
	 *
	 * @param leftOperand	The left operand of the binary comparison
	 * @param rightOperand	The right operand of the binary comparison
	 * @param truth			The truth value of the comparison
	 *
	 * @return	A SQLBoolean containing the desired truth value.
	 */

	public static SQLBoolean truthValue(
								DataValueDescriptor leftOperand,
								DataValueDescriptor rightOperand,
								boolean truth)
	{
		/* Return UNKNOWN if either operand is null */
		if (leftOperand.isNull() || rightOperand.isNull())
		{
			return unknownTruthValue();
		}

		/* Return the appropriate SQLBoolean for the given truth value */
		if (truth == true)
		{
			return BOOLEAN_TRUE;
		}
		else
		{
			return BOOLEAN_FALSE;
		}
	}

    /**
     * same as above, but takes a Boolean, if it is null, unknownTruthValue is returned
     */
	public static SQLBoolean truthValue(
								DataValueDescriptor leftOperand,
								DataValueDescriptor rightOperand,
								Boolean truth)
	{
		/* Return UNKNOWN if either operand is null */
		if (leftOperand.isNull() || rightOperand.isNull() || truth==null)
		{
			return unknownTruthValue();
		}

		/* Return the appropriate SQLBoolean for the given truth value */
		if (truth == Boolean.TRUE)
		{
			return BOOLEAN_TRUE;
		}
		else
		{
			return BOOLEAN_FALSE;
		}
	}

	/**
	 * Get a truth value.
	 *
	 * @param value	The value of the SQLBoolean
	 *
 	 * @return	A SQLBoolean with the given truth value
	 */
	public static SQLBoolean truthValue(boolean value)
	{
		/*
		** Return the non-nullable versions of TRUE and FALSE, since they
		** can never be null.
		*/
		if (value == true)
			return BOOLEAN_TRUE;
		else
			return BOOLEAN_FALSE;
	}

	/**
	 * Return an unknown truth value.  Check to be sure the return value is
	 * nullable.
	 *
	 * @return	A SQLBoolean representing the UNKNOWN truth value
	 */
	public static SQLBoolean unknownTruthValue()
	{
		return UNKNOWN;
	}

	/**
	 * Determine whether this SQLBoolean contains the given boolean value.
	 *
	 * This method is used by generated code to determine when to do
	 * short-circuiting for an AND or OR.
	 *
	 * @param val	The value to look for
	 *
	 * @return	true if the given value equals the value in this SQLBoolean,
	 *			false if not
	 */

	public boolean equals(boolean val)
	{
		if (isNull())
			return false;
		else
			return value == val;
	}
	
	/**
	 * Return an immutable BooleanDataValue with the same value as this.
	 * @return An immutable BooleanDataValue with the same value as this.
	 */
	public BooleanDataValue getImmutable()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-742
		if (isNull())
			return SQLBoolean.UNKNOWN;
		
		return value ? SQLBoolean.BOOLEAN_TRUE : SQLBoolean.BOOLEAN_FALSE;
	}

	/*
	 * String display of value
	 */

	public String toString()
	{
		if (isNull())
			return "NULL";
		else if (value == true)
			return "true";
		else
			return "false";
	}

	/*
	 * Hash code
	 */
	public int hashCode()
	{
		if (isNull())
		{
			return -1;
		}

		return (value) ? 1 : 0;
	}

	/*
	 * useful constants...
	 */
	static final int BOOLEAN_LENGTH		= 1;	// must match the number of bytes written by DataOutput.writeBoolean()

	private static final SQLBoolean BOOLEAN_TRUE = new SQLBoolean(true);
	private static final SQLBoolean BOOLEAN_FALSE = new SQLBoolean(false);
	static final SQLBoolean UNKNOWN = new SQLBoolean();

	/* Static initialization block */
	static
	{
		/* Mark all the static SQLBooleans as immutable */
		BOOLEAN_TRUE.immutable = true;
		BOOLEAN_FALSE.immutable = true;
		UNKNOWN.immutable = true;
	}

    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( SQLBoolean.class);

    public int estimateMemoryUsage()
    {
        return BASE_MEMORY_USAGE;
    }

	/*
	 * object state
	 */
	private boolean value;
	private boolean isnull;
	private boolean immutable;
}
