/*

   Derby - Class org.apache.derby.impl.store.access.conglomerate.GenericConglomerate

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

package org.apache.derby.impl.store.access.conglomerate;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException; 

import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.Orderable;

import org.apache.derby.iapi.types.DataType;

import java.sql.ResultSet;
import java.sql.SQLException;

/**

A class that implements the methods shared across all implementations of
the Conglomerate interface.

**/

public abstract class GenericConglomerate 
    extends DataType implements Conglomerate
{

    /**************************************************************************
     * Public Methods implementing DataValueDescriptor interface.
     **************************************************************************
     */

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
     * 
     * @see org.apache.derby.iapi.types.DataValueDescriptor#getLength
	 */
	public int	getLength() 
        throws StandardException
    {
        throw(StandardException.newException(
                SQLState.HEAP_UNIMPLEMENTED_FEATURE));
    }
	/**
	 * Gets the value in the data value descriptor as a String.
	 * Throws an exception if the data value is not a string.
	 *
	 * @return	The data value as a String.
	 *
	 * @exception StandardException   Thrown on error
     *
     * @see org.apache.derby.iapi.types.DataValueDescriptor#getString
	 */
	public String	getString() throws StandardException
    {
        throw(StandardException.newException(
                SQLState.HEAP_UNIMPLEMENTED_FEATURE));
    }

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
     *
     * @see org.apache.derby.iapi.types.DataValueDescriptor#getObject
	 */
	public Object	getObject() throws StandardException
    {
        return(this);
    }

	/**
	 * <U>Shallow copy</U>. 
	 * <p> 
	 * Clone the DataValueDescriptor and copy its contents.
	 * We clone the data value wrapper (e.g. SQLDecimal)
	 * and reuse its contents (the underlying BigDecimal).
	 * The resultant DataValueDescriptor will point to the same
	 * value as the original DataValueDescriptor (unless the value
	 * is a primitive type, e.g. SQLInteger/integer).
	 *
	 * @return A clone of the DataValueDescriptor reusing its contents.
     *
     * @see org.apache.derby.iapi.types.DataValueDescriptor#getClone
	 */
	public DataValueDescriptor getClone()
    {
        if (SanityManager.DEBUG)
            SanityManager.THROWASSERT("Not implemented!.");

        return(null);
    }

	/**
	 * Get a new null value of the same type as this data value.
	 *
     * @see org.apache.derby.iapi.types.DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
    {
        if (SanityManager.DEBUG)
            SanityManager.THROWASSERT("Not implemented!.");

        return(null);
    }

	/**
	 * Set the value based on the value for the specified DataValueDescriptor
	 * from the specified ResultSet.
	 *
	 * @param resultSet		The specified ResultSet.
	 * @param colNumber		The 1-based column # into the resultSet.
	 * @param isNullable	Whether or not the column is nullable
	 *						(No need to call wasNull() if not)
	 * 
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 * @exception SQLException		Error accessing the result set
     *
     * @see org.apache.derby.iapi.types.DataValueDescriptor#setValueFromResultSet
	 */
	public void setValueFromResultSet(
    ResultSet   resultSet, 
    int         colNumber,
    boolean     isNullable)
		throws StandardException, SQLException
    {
        throw(StandardException.newException(
                SQLState.HEAP_UNIMPLEMENTED_FEATURE));
    }

	/**
	 * Set the value of this DataValueDescriptor to the given value
	 *
	 * @param theValue	An Object containing the value to set this
	 *					DataValueDescriptor to.  Null means set the value
	 *					to SQL null.
	 *
	 * @return	This DataValueDescriptor
	 *
	 * @exception StandardException		Thrown on error
     *
     * @see org.apache.derby.iapi.types.DataValueDescriptor#setValue
	 */ 
	public void setValue(Object theValue) 
        throws StandardException
    {
        throw(StandardException.newException(
                SQLState.HEAP_UNIMPLEMENTED_FEATURE));
    }

	/**
	 * Set the value of this DataValueDescriptor from another.
	 *
	 * @param theValue	The Date value to set this DataValueDescriptor to
	 *
	 * @return	This DataValueDescriptor
	 *
     * @see org.apache.derby.iapi.types.DataValueDescriptor#setValue
	 */
	protected void setFrom(DataValueDescriptor theValue) 
        throws StandardException
    {
        throw(StandardException.newException(
                SQLState.HEAP_UNIMPLEMENTED_FEATURE));
    }

	/**
	 * Get the SQL name of the datatype
	 *
	 * @return	The SQL name of the datatype
     *
     * @see org.apache.derby.iapi.types.DataValueDescriptor#getTypeName
	 */
	public String	getTypeName()
    {
        if (SanityManager.DEBUG)
            SanityManager.THROWASSERT("Not implemented!.");

        return(null);
    }

	/**
	 * Compare this Orderable with a given Orderable for the purpose of
	 * index positioning.  This method treats nulls as ordered values -
	 * that is, it treats SQL null as equal to null and less than all
	 * other values.
	 *
	 * @param other		The Orderable to compare this one to.
	 *
	 * @return  <0 - this Orderable is less than other.
	 * 			 0 - this Orderable equals other.
	 *			>0 - this Orderable is greater than other.
     *
     *			The code should not explicitly look for -1, or 1.
	 *
	 * @exception StandardException		Thrown on error
     *
     * @see DataValueDescriptor#compare
	 */
	public int compare(DataValueDescriptor other) 
        throws StandardException
	{
        throw(StandardException.newException(
                SQLState.HEAP_UNIMPLEMENTED_FEATURE));
	}
}
