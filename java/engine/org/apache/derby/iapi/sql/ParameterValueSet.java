/*

   Derby - Class org.apache.derby.iapi.sql.ParameterValueSet

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.sql;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * A ParameterValueSet is a set of parameter values that can be assembled by a
 * JDBC driver and passed to a PreparedStatement all at once. The fact that
 * they are all passed at once can reduce the communication overhead between
 * client and server.
 *
 * @author Jeff Lichtman
 */
public interface ParameterValueSet
{


	/**
		Set the mode of the parameter, called when setting up static method calls and stored procedures.
		Otherwise the parameter type will default to an IN parameter.
	*/
    void setParameterMode(int position, int mode);

	/**
	 * Set a parameter position to a DataValueDescriptor.
	 *
	 * NOTE: This method assumes the caller will not pass a position that's
	 * out of range.  The implementation may have an assertion that the position
	 * is in range.
	 *
	 * @param sdv		The DataValueDescriptor to set
	 * @param position	The parameter position to set it at
	 * @param jdbcTypeId    The corresponding JDBC types from java.sql.Types
	 * @param className  The declared class name for the type.
	 */

	void setStorableDataValue(DataValueDescriptor sdv, int position, int jdbcTypeId, String className);


	//////////////////////////////////////////////////////////////////
	//
	// CALLABLE STATEMENT
	//
	//////////////////////////////////////////////////////////////////

	/**
	 * Mark the parameter as an output parameter.
	 *
	 * @param parameterIndex	The ordinal position of a parameter to set
	 *			to the given value.
	 * @param sqlType	A type from java.sql.Types
	 * @param scale		the scale to use.  -1 means ignore scale
	 *
	 * @exception StandardException on error
	 */
	void registerOutParameter(int parameterIndex, int sqlType, int scale)
		throws StandardException;


    /**
     * Get the value of a parameter as a Java object.
     *
     * <p>This method returns a Java object whose type coresponds to the SQL
     * type that was registered for this parameter using registerOutParameter.
     *
     * <p>Note that this method may be used to read
     * datatabase-specific, abstract data types. This is done by
     * specifying a targetSqlType of java.sql.types.OTHER, which
     * allows the driver to return a database-specific Java type.
     *
     * @param parameterIndex The first parameter is 1, the second is 2, ...
     * @return A java.lang.Object holding the OUT parameter value.
     * @exception StandardException if a database-access error occurs.
     * @see java.sql.Types 
     */
    Object getObject(int parameterIndex) throws StandardException;

	//////////////////////////////////////////////////////////////////
	//
	// MISC STATEMENT
	//
	//////////////////////////////////////////////////////////////////

	/**
	 * Sets all parameters to an uninitialized state. An exception will be
	 * thrown if the caller tries to execute a PreparedStatement when one
	 * or more parameters is uninitialized (i.e. has not had
	 * setParameterValue() called on it.
	 *
	 * @return	Nothing
	 */
	void	clearParameters();

	/**
	 * Returns the number of parameters in this set.
	 *
	 * @return	The number of parameters in this set.
	 */
	public	int	getParameterCount();

	/**
	 * Returns the parameter at the given position.
	 *
	 * @return	The parameter at the given position.
	 * @exception StandardException		Thrown on error
	 */
	public DataValueDescriptor getParameter( int position ) throws StandardException;


	/**
	 * Returns the parameter at the given position in order to set it.
	   Setting via an unknown object type must use setParameterAsObject()
	   to ensure correct typing.

	 *
	 * @return	The parameter at the given position.
	 * @exception StandardException		Thrown on error
	 */
	public DataValueDescriptor getParameterForSet( int position ) throws StandardException;

	/**
		Set the value of this parameter to the passed in Object.
		
		  @return	The parameter at the given position.
		  @exception StandardException		Thrown on error
	*/
	void setParameterAsObject(int parameterIndex, Object value) throws StandardException;
	
	
	public DataValueDescriptor getParameterForGet( int position ) throws StandardException;

	/**
	 * Tells whether all the parameters are set and ready for execution.
	   OUT and Cloudscape static method INOUT parameters are not required to be set.
	 *
	 * @return	true if all parameters are set, false if at least one
	 *			parameter is not set.
	 */
	boolean	allAreSet();

	/**
	 * Clone the ParameterValueSet and its contents.
	 *
	 * @return ParameterValueSet	A clone of the ParameterValueSet and its contents.
	 */
	ParameterValueSet getClone();

	/**
	 * Validate the parameters.  This is done for situations where
	 * we cannot validate everything in the setXXX() calls.  In
	 * particular, before we do an execute() on a CallableStatement,
	 * we need to go through the parameters and make sure that
	 * all parameters are set up properly.  The motivator for this
	 * is that setXXX() can be called either before or after 
	 * registerOutputParamter(), we cannot be sure we have the types
	 * correct until we get to execute().
	 *
	 * @exception StandardException if the parameters aren't valid
	 */
	void validate() throws StandardException;

	/**
	 * Is there a return output parameter in this pvs.  A return
	 * parameter is from a CALL statement of the following
	 * syntax: ? = CALL myMethod().  Note that a return
	 * output parameter is NOT the same thing as an output
	 * parameter; it is a special type of output parameter.
	 *
	 * @return true if it has a return parameter
	 *
	 */
	public boolean hasReturnOutputParameter();

	/**
		Check that there are not output parameters defined
		by the parameter set. If there are unknown parameter
		types they are forced to input types. i.e. Cloudscape static method
		calls with parameters that are array.

		@return true if a declared Java Procedure INOUT or OUT parameter is in the set, false otherwise.
	*/
	public boolean checkNoDeclaredOutputParameters();


	// bug 4552 - "exec statement using" will return no parameters through parametermetadata
	/**
	 * Is this pvs for using clause.
	 *
	 * @return true if it has a output parameter
	 *
	 */
	public boolean isUsingParameterValueSet();

	// bug 4552 - "exec statement using" will return no parameters through parametermetadata
	/**
	 * Setthis pvs for using clause.
	 */
	public void setUsingParameterValueSet();

	/**
	 * Set the parameter values of the pvstarget to equal those 
	 * set in this PVS.
	 * Used to transfer saved SPS parameters to the actual
	 * prepared statement parameters  once associated parameters 
	 * have been established.  Assumes pvstarget is the same 
	 * length as this.
	 * @param pvstarget ParameterValueSet which will recieve the values

		@exception StandardException values not compatible
	 **/
	public void transferDataValues(ParameterValueSet pvstarget) throws StandardException;

	/**
		Return the mode of the parameter according to JDBC 3.0 ParameterMetaData
		
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 *
	 */
	public short getParameterMode(int parameterIndex);


    /**
     * Set the value of the return parameter as a Java object.
     *
     * @param value the return value
     *
     * @exception StandardException if a database-access error occurs.
     */
	void setReturnValue(Object value) throws StandardException;

	/**
	 * Return the scale of the given parameter index in this pvs.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 *
	 * @return scale
	 */
	public int getScale(int parameterIndex);

	/**
	 * Return the precision of the given parameter index in this pvs.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 *
	 * @return precision
	 */
	public int getPrecision(int parameterIndex);


}

