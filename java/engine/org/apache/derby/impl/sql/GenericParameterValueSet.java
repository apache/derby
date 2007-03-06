/*

   Derby - Class org.apache.derby.impl.sql.GenericParameterValueSet

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

package org.apache.derby.impl.sql;

import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.loader.ClassInspector;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.ParameterValueSet;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.UserDataValue;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.io.InputStream;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import org.apache.derby.iapi.reference.JDBC30Translation;

/**
 * Implementation of ParameterValueSet
 *
 * @see ParameterValueSet
 *
 */

final class GenericParameterValueSet implements ParameterValueSet 
{
  //all this has to be copied in the clone constructor
	private final GenericParameter[]				parms;
	final ClassInspector 			ci;
	private	final boolean			hasReturnOutputParam;


	/**
	 * Constructor for a GenericParameterValueSet
	 *
	 * @param numParms	The number of parameters in the new ParameterValueSet
	 * @param hasReturnOutputParam	if we have a ? = call syntax.  Note that
	 *			this is NOT the same thing as an output parameter -- return
	 *			output parameters are special cases of output parameters.
	 */
	GenericParameterValueSet(ClassInspector ci, int numParms, boolean hasReturnOutputParam)
	{
		this.ci = ci;
		this.hasReturnOutputParam = hasReturnOutputParam;
		parms = new GenericParameter[numParms];
		for (int i = 0; i < numParms; i++)
		{
			/*
			** Last param is if this is a return output param.  True if 
			** we have an output param and we are on the 1st parameter.
			*/	
			parms[i] = new GenericParameter(this, (hasReturnOutputParam && i == 0));
		}
	}

	/*
	** Construct a pvs by cloning a pvs.
	*/
	private GenericParameterValueSet(int numParms, GenericParameterValueSet pvs)
	{
		this.hasReturnOutputParam = pvs.hasReturnOutputParam;
		this.ci = pvs.ci;
		parms = new GenericParameter[numParms];
		for (int i = 0; i < numParms; i++)
		{
			parms[i] = pvs.getGenericParameter(i).getClone(this);
		}
	}

	/*
	** ParameterValueSet interface methods
	*/
	
	/**
	 * Initialize the set by allocating a holder DataValueDescriptor object
	 * for each parameter.
	 */
	public void initialize(DataTypeDescriptor[] types)
	{
		for (int i = 0; i < parms.length; i++)
		{
			DataTypeDescriptor dtd = types[i];
			
			parms[i].initialize(dtd.getNull(),
					dtd.getJDBCTypeId(), dtd.getTypeId().getCorrespondingJavaTypeName());
		}
	}

	public void setParameterMode(int position, int mode) {
		parms[position].parameterMode = (short) mode;
	}

	/**
	 * @see ParameterValueSet#clearParameters
	 */
	public void clearParameters()
	{
		for (int i = 0; i < parms.length; i++)
		{
			parms[i].clear();
		}
	}

	/**
	 * Returns the number of parameters in this set.
	 *
	 * @return	The number of parameters in this set.
	 */
    public	int	getParameterCount()
	{
		return parms.length;
	}

	/**
	 * Returns the parameter value at the given position.
	 *
	 * @return	The parameter at the given position.
	 * @exception StandardException		Thrown on error
	 */
	public	DataValueDescriptor	getParameter( int position ) throws StandardException
	{
		try {
			return parms[position].getValue();
		} catch (ArrayIndexOutOfBoundsException e) {
			checkPosition(position);
			return null;
		}
	}



	public	DataValueDescriptor	getParameterForSet(int position) throws StandardException {

		try {

			GenericParameter gp = parms[position];
			if (gp.parameterMode == JDBC30Translation.PARAMETER_MODE_OUT)
				throw StandardException.newException(SQLState.LANG_RETURN_OUTPUT_PARAM_CANNOT_BE_SET);

			gp.isSet = true;

			return gp.getValue();
		} catch (ArrayIndexOutOfBoundsException e) {
			checkPosition(position);
			return null;
		}

	}
	
	public	DataValueDescriptor	getParameterForGet(int position) throws StandardException {

		try {

			GenericParameter gp = parms[position];

			switch (gp.parameterMode) {
			case JDBC30Translation.PARAMETER_MODE_IN:
			case JDBC30Translation.PARAMETER_MODE_UNKNOWN:
				throw StandardException.newException(SQLState.LANG_NOT_OUTPUT_PARAMETER, Integer.toString(position + 1));
			}

			return gp.getValue();
		} catch (ArrayIndexOutOfBoundsException e) {
			checkPosition(position);
			return null;
		}
	}

	public void setParameterAsObject(int position, Object value) throws StandardException {

		UserDataValue dvd = (UserDataValue) getParameterForSet(position);

		GenericParameter gp = parms[position];
		if (value != null) {

			{

				boolean throwError;
				ClassNotFoundException t = null;
				try {
					throwError = !ci.instanceOf(gp.declaredClassName, value);
				} catch (ClassNotFoundException cnfe) {
					t = cnfe;
					throwError = true;
				}

				if (throwError) {
					throw StandardException.newException(SQLState.LANG_DATA_TYPE_SET_MISMATCH, t,
						ClassInspector.readableClassName(value.getClass()), gp.declaredClassName);
				}
			}

		}

		dvd.setValue(value);
	}

	/**
	 * @see ParameterValueSet#allAreSet
	 */
	public boolean allAreSet()
	{
		for (int i = 0; i < parms.length; i++)
		{
			GenericParameter gp = parms[i];
			if (!gp.isSet)
			{
				switch (gp.parameterMode) {
				case JDBC30Translation.PARAMETER_MODE_OUT:
					break;
				case JDBC30Translation.PARAMETER_MODE_IN_OUT:
				case JDBC30Translation.PARAMETER_MODE_UNKNOWN:
				case JDBC30Translation.PARAMETER_MODE_IN:
					return false;
				}
			}
		}

		return true;
	}

	/**
	 * @see ParameterValueSet#transferDataValues
	 */
	public void  transferDataValues(ParameterValueSet pvstarget) throws StandardException
	{
		// don't take application's values for return output parameters
		int firstParam = pvstarget.hasReturnOutputParameter() ? 1 : 0;
		for (int i = firstParam; i < parms.length;i++)
		{

			GenericParameter oldp = parms[i];

			if (oldp.registerOutType != Types.NULL) {

				pvstarget.registerOutParameter(i, oldp.registerOutType, oldp.registerOutScale);

			}

			if (oldp.isSet)
			{
				pvstarget.getParameterForSet(i).setValue(oldp.getValue());
			}
		}
	}

	GenericParameter getGenericParameter(int position)
	{
    return(parms[position]);
  }

	/* Class implementation */
	public String toString()
	{
		/* This method needed for derby.language.logStatementText=true.
		 * Do not put under SanityManager.DEBUG.
		 */
		StringBuffer strbuf = new StringBuffer();

		for (int ctr = 0; ctr < parms.length; ctr++)
		{
			strbuf.append("begin parameter #" + (ctr + 1) + ": ");
			strbuf.append(parms[ctr].toString());
			strbuf.append(" :end parameter ");
		}

		return strbuf.toString();
	}

	/**
	 * Check the position number for a parameter and throw an exception if
	 * it is out of range.
	 *
	 * @param position	The position number to check
	 *
	 * @exception StandardException	Thrown if position number is
	 *											out of range.
	 */
	private void checkPosition(int position) throws StandardException
	{
		if (position < 0 || position >= parms.length)
		{

			if (parms.length == 0)
				throw StandardException.newException(SQLState.NO_INPUT_PARAMETERS);

			throw StandardException.newException(SQLState.LANG_INVALID_PARAM_POSITION, 
															String.valueOf(position+1),
															String.valueOf(parms.length));
		}
	}


	public ParameterValueSet getClone()
	{
		return(new GenericParameterValueSet(parms.length, this));
	}

	//////////////////////////////////////////////////////////////////
	//
	// CALLABLE STATEMENT
	//
	//////////////////////////////////////////////////////////////////

	/**
	 * Mark the parameter as an output parameter.
	 *
	 * @param parameterIndex	The ordinal parameterIndex of a parameter to set
	 *			to the given value.
	 * @param sqlType	A type from java.sql.Types
	 * @param scale		the scale to use.  -1 means ignore scale
	 *
	 * @exception StandardException on error
	 */
	public void registerOutParameter(int parameterIndex, int sqlType, int scale)
		throws StandardException
	{
		checkPosition(parameterIndex);
		parms[parameterIndex].setOutParameter(sqlType, scale);
	}

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
	public void validate() throws StandardException
	{
		for (int i = 0; i < parms.length; i++)
		{
			parms[i].validate();
		}
	}


	/**
	 * Return the parameter number (in jdbc lingo, i.e. 1 based)
	 * for the given parameter.  Linear search. 
	 *
	 * @return the parameter number, or 0 if not found
	 */
	public int getParameterNumber(GenericParameter theParam)
	{
		for (int i = 0; i < parms.length; i++)
		{
			if (parms[i] == theParam)
			{
				return i+1;
			}
		}
		return 0;
	}

	/**
		Check that there are not output parameters defined
		by the parameter set. If there are unknown parameter
		types they are forced to input types. i.e. Cloudscape static method
		calls with parameters that are array.

		@return true if a declared Java Procedure INOUT or OUT parameter is in the set, false otherwise.
	*/
	public boolean checkNoDeclaredOutputParameters() {

		boolean hasDeclaredOutputParameter = false;
		for (int i=0; i<parms.length; i++) {

			GenericParameter gp = parms[i];

			switch (gp.parameterMode) {
			case JDBC30Translation.PARAMETER_MODE_IN:
				break;
			case JDBC30Translation.PARAMETER_MODE_IN_OUT:
			case JDBC30Translation.PARAMETER_MODE_OUT:
				hasDeclaredOutputParameter = true;
				break;
			case JDBC30Translation.PARAMETER_MODE_UNKNOWN:
				gp.parameterMode = JDBC30Translation.PARAMETER_MODE_IN;
				break;
			}
		}
		return hasDeclaredOutputParameter;
	}

	/**
		Return the mode of the parameter according to JDBC 3.0 ParameterMetaData
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 *
	 */
	public short getParameterMode(int parameterIndex)
	{
		short mode = parms[parameterIndex - 1].parameterMode;
		//if (mode == (short) JDBC30Translation.PARAMETER_MODE_UNKNOWN)
		//	mode = (short) JDBC30Translation.PARAMETER_MODE_IN;
		return mode;
	}

	/**
	 * Is there a return output parameter in this pvs.  A return
	 * parameter is from a CALL statement of the following
	 * syntax: ? = CALL myMethod()
	 *
	 * @return true if it has a return parameter
	 *
	 */
	public boolean hasReturnOutputParameter()
	{
		return hasReturnOutputParam;
	}

    /**
     * Get the value of the return parameter in order to set it.
     *
 	 *
     * @exception StandardException if a database-access error occurs.
     */
    public DataValueDescriptor getReturnValueForSet() throws StandardException
	{
		checkPosition(0);
				
		if (SanityManager.DEBUG)
		{
			if (!hasReturnOutputParam)
				SanityManager.THROWASSERT("getReturnValueForSet called on non-return parameter");
		}
		
		return parms[0].getValue();
	}

	/**
	 * Return the scale of the given parameter index in this pvs.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 *
	 * @return scale
	 */
	public int getScale(int parameterIndex)
	{
		return parms[parameterIndex-1].getScale();
	}

	/**
	 * Return the precision of the given parameter index in this pvs.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 *
	 * @return precision
	 */
	public int getPrecision(int parameterIndex)
	{
		return parms[parameterIndex-1].getPrecision();
	}

}
