/*

   Derby - Class org.apache.derby.impl.sql.GenericParameter

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

package org.apache.derby.impl.sql;

import org.apache.derby.iapi.services.loader.ClassInspector;

import org.apache.derby.iapi.sql.ParameterValueSet;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.BitDataValue;
import org.apache.derby.iapi.types.DateTimeDataValue;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.types.UserDataValue;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.*;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.reference.JDBC30Translation;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.types.*;
import org.apache.derby.iapi.types.*;

import java.sql.Types;

import java.lang.reflect.Array;

/**
 * A parameter.  Originally lifted from ParameterValueSet.
 *
 * @author jamie 
 */
final class GenericParameter
{

	// These defaults match the Network Server/ JCC max precision and
	// The JCC "guessed" scale. They are used as the defaults for 
	// Decimal out params.
	private static int DECIMAL_PARAMETER_DEFAULT_PRECISION = 31;
	private static int DECIMAL_PARAMETER_DEFAULT_SCALE = 15;


	/*
	** The parameter set we are part of
	*/
	private final GenericParameterValueSet		pvs;

	/**
	** Our value
	*/
	private DataValueDescriptor				value;

	/**
		Compile time JDBC type identifier.
	*/
	int								jdbcTypeId;

	/**
		Compile time Java class name.
	*/
	String							declaredClassName;

	/**
		Mode of the parameter, from ParameterMetaData
	*/
	short							parameterMode;

	/*
	** If we are set
	*/
	boolean							isSet;

	/*
	** Output parameter values
 	*/
	private final boolean					isReturnOutputParameter;

	/**
		Type that has been registered.
	*/
	int	registerOutType = Types.NULL;
	/**
		Scale that has been registered.
	*/
	int registerOutScale = -1;

	/**
	 * When a decimal output parameter is registered we give it a 
	 * precision
	 */

	int registerOutPrecision = -1;

	/**
	 * Constructor for a Parameter
	 *
	 * @param pvs the parameter set that this is part of
	 * @param isReturnOutputParameter true if this is a return output parameter
	 */
	GenericParameter
	(
		GenericParameterValueSet	pvs,
		boolean						isReturnOutputParameter
	)
	{
		this.pvs = pvs;
		parameterMode = (this.isReturnOutputParameter = isReturnOutputParameter)
			? (short) JDBC30Translation.PARAMETER_MODE_OUT : (short) JDBC30Translation.PARAMETER_MODE_IN;
	}

	/**
	 * Clone myself.  It is a shallow copy for everything but
	 * the underlying data wrapper and its value -- e.g. for
	 * everything but the underlying SQLInt and its int.
	 *
	 * @param pvs the parameter value set
	 *
	 * @return a new generic parameter.
	 */
	public GenericParameter getClone(GenericParameterValueSet pvs)
	{
		GenericParameter gpClone = new GenericParameter(pvs, isReturnOutputParameter);
		gpClone.setStorableDataValue(this.getValue().getClone(), jdbcTypeId, declaredClassName);
		gpClone.isSet = true;

		return gpClone;
	}

	/**
	 * Set the DataValueDescriptor for this parameter.  If makrAsSet
	 * is set, then we consider this parameter both initialized and
	 * set.  Otherwise, it is just initialized but not yet set.
	 *
	 * @param std the storable data value
	 * @param markAsSet whether we should mark as being set or not.
	 * 		The isSet() method will return this value.
	 */
	void setStorableDataValue(DataValueDescriptor value, int jdbcTypeId, String className)
	{
		this.value = value;
		this.jdbcTypeId = jdbcTypeId;
		this.declaredClassName = className;
	}


	/**
	 * Clear the parameter, unless it is a return
	 * output parameter
	 */
	void clear()
	{
		isSet = false;
	}


	/**
	 * Get the parameter value.  Doesn't check to
	 * see if it has been initialized or not.
	 *
	 * @return the parameter value, may return null
	 */
	DataValueDescriptor getValue()
	{
		return value;
	}


	//////////////////////////////////////////////////////////////////
	//
	// CALLABLE STATEMENT
	//
	//////////////////////////////////////////////////////////////////

	/**
	 * Mark the parameter as an output parameter.
	 *
	 * @param sqlType	A type from java.sql.Types
	 * @param scale		scale, -1 if no scale arg
	 *
	 * @exception StandardException on error
	 */
	void setOutParameter(int sqlType, int scale)
		throws StandardException
	{
		// fast case duplicate registrations.
		if (registerOutType == sqlType) {
			if (scale == registerOutScale)
				return;
		}

		switch (parameterMode) {
		case JDBC30Translation.PARAMETER_MODE_IN:
		case JDBC30Translation.PARAMETER_MODE_UNKNOWN:
		default:
			throw StandardException.newException(SQLState.LANG_NOT_OUT_PARAM, getJDBCParameterNumberStr());

		case JDBC30Translation.PARAMETER_MODE_IN_OUT:
		case JDBC30Translation.PARAMETER_MODE_OUT:
			// Declared/Java procedure parameter.
			if (!DataTypeDescriptor.isJDBCTypeEquivalent(jdbcTypeId, sqlType))
				throw throwInvalidOutParamMap(sqlType);
			break;

		}

		registerOutType = sqlType;
		
	}

	private StandardException throwInvalidOutParamMap(int sqlType) {

		//TypeId typeId = TypeId.getBuiltInTypeId(sqlType);
		// String sqlTypeName = typeId == null ? "OTHER" : typeId.getSQLTypeName();


		String jdbcTypesName = org.apache.derby.impl.jdbc.Util.typeName(sqlType);

		TypeId typeId = TypeId.getBuiltInTypeId(jdbcTypeId);
		String thisTypeName = typeId == null ? declaredClassName : typeId.getSQLTypeName();
				
		StandardException e = StandardException.newException(SQLState.LANG_INVALID_OUT_PARAM_MAP,
					getJDBCParameterNumberStr(),
					jdbcTypesName, thisTypeName);

		return e;
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
	void validate() throws StandardException
	{
		switch (parameterMode) {
		case JDBC30Translation.PARAMETER_MODE_UNKNOWN:
			break;
		case JDBC30Translation.PARAMETER_MODE_IN:
			break;
		case JDBC30Translation.PARAMETER_MODE_IN_OUT:
		case JDBC30Translation.PARAMETER_MODE_OUT:
			if (registerOutType == Types.NULL) {
				throw StandardException.newException(SQLState.NEED_TO_REGISTER_PARAM,
					getJDBCParameterNumberStr(),
					 org.apache.derby.catalog.types.RoutineAliasInfo.parameterMode(parameterMode));
			}
			break;
		}
	}

	/**
	 * Return the scale of the parameter.
	 *
	 * @return scale
	 */
	int getScale()
	{
		//when the user doesn't pass any scale, the registerOutScale gets set to -1
		return (registerOutScale == -1 ? 0 : registerOutScale);
	}


	int getPrecision()
	{
		return registerOutPrecision;
		
	}

	////////////////////////////////////////////////////
	//
	// CLASS IMPLEMENTATION
	//
	////////////////////////////////////////////////////

	/**
	 * do a setValue on the particular field.  Doesn't do
	 * checks on what it is being set to or on whether
	 * it is a return parameter or what.  This is only
	 * called internally; it is not expected to be called
	 * directly as a result of some jdbc call (e.g. setObject()).
	 *
	 * @param newValue the value to set
	 *
	 * @exception StandardException on error
	 */
	void stuffObject(Object newValue) throws StandardException
	{
		value.setValue(newValue);
		isSet = true;
	}

	/**
	 * get string for param number
	 */
	String getJDBCParameterNumberStr()
	{
		return Integer.toString(pvs.getParameterNumber(this));
	}

	public String toString()
	{
		/* This method is used for debugging.
		 * It is called when derby.language.logStatementText=true,
		 * so there is no check of SanityManager.DEBUG.
		 * Anyway, we need to call value.getString() instead of
		 * value.toString() because the user may have done a
		 * a setStream() on the parameter.  (toString() could get
		 * an assertion failure in that case as it would be in an
		 * unexpected state since this is a very weird codepath.)
		 * getString() can throw an exception which we eat and
		 * and reflect in the returned string.
		 */
		if (value == null)
		{
			return "null";
		}
		else
		{
			try
			{
				return value.getString();
			}
			catch (StandardException se)
			{
				return "unexpected exception from getString() - " + se;
			}
		}
	}
}
