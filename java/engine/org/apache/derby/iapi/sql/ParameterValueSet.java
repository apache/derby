/*

   Derby - Class org.apache.derby.iapi.sql.ParameterValueSet

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

package org.apache.derby.iapi.sql;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * A ParameterValueSet is a set of parameter values that can be assembled by a
 * JDBC driver and passed to a PreparedStatement all at once. The fact that
 * they are all passed at once can reduce the communication overhead between
 * client and server.
 *
 */
public interface ParameterValueSet
{
	/**
	 * Initialize the parameter set by allocating DataValueDescriptor
	 * corresponding to the passed in type for each parameter.
	 * @param types expected to match the number of parameters.
	 */
	void initialize(DataTypeDescriptor[] types) throws StandardException;


	/**
		Set the mode of the parameter, called when setting up static method calls and stored procedures.
		Otherwise the parameter type will default to an IN parameter.
	*/
    void setParameterMode(int position, int mode);

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
		Set the value of this user defined parameter to the passed in Object.
		
		  @exception StandardException		Thrown on error
	*/
	void setParameterAsObject(int parameterIndex, Object value) throws StandardException;
	
	/**
	 * Get the DataValueDescriptor for an INOUT or OUT parameter.
	 * @param position Zero based index of the parameter.
	 * @return Parameter's value holder.
	 * @throws StandardException Position out of range or the parameter is not INOUT or OUT.
	 */
	public DataValueDescriptor getParameterForGet( int position ) throws StandardException;

	/**
	 * Tells whether all the parameters are set and ready for execution.
	   OUT are not required to be set.
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
		types they are forced to input types. i.e. Derby static method
		calls with parameters that are array.

		@return true if a declared Java Procedure INOUT or OUT parameter is in the set, false otherwise.
	*/
	public boolean checkNoDeclaredOutputParameters();

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
     * Get the value of the return parameter in order to set it.
     *
     *
     * @exception StandardException if a database-access error occurs.
     */
	DataValueDescriptor getReturnValueForSet() throws StandardException;

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

