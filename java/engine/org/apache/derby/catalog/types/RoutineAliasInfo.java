/*

   Derby - Class org.apache.derby.catalog.types.RoutineAliasInfo

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.catalog.types;

import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.JDBC30Translation;
import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.catalog.TypeDescriptor;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * Describe a r (procedure or function) alias.
 *
 * @see AliasInfo
 */
public class RoutineAliasInfo extends MethodAliasInfo
{

	private static final String[] SQL_CONTROL = {"MODIFIES SQL DATA", "READS SQL DATA", "CONTAINS SQL", "NO SQL"};
	public static final short MODIFIES_SQL_DATA = 0;
	public static final short READS_SQL_DATA	= 1;
	public static final short CONTAINS_SQL		= 2;
	public static final short NO_SQL			= 3;



	/** PARAMETER STYLE JAVA */
	public static final short PS_JAVA = 0;

	private int parameterCount;

	private TypeDescriptor[]	parameterTypes;
	private String[]			parameterNames;
	/**
		IN, OUT, INOUT
	*/
	private int[]				parameterModes;

	private int dynamicResultSets;

	/**
		Return type for functions. Null for procedures.
	*/
	private TypeDescriptor	returnType;

	/**
		Parameter style - always PS_JAVA at the moment.
	*/
	private short parameterStyle;

	/**
		What SQL is allowed by this procedure.
	*/
	private short	sqlAllowed;

	/**
		SQL Specific name (future)
	*/
	private String	specificName;

	/**
		True if the routine is called on null input.
		(always true for procedures).
	*/
	private boolean	calledOnNullInput;

	public RoutineAliasInfo() {
	}

	/**
		Create a RoutineAliasInfo for an internal PROCEDURE.
	*/
	public RoutineAliasInfo(String methodName, int parameterCount, String[] parameterNames,
		TypeDescriptor[]	parameterTypes, int[] parameterModes, int dynamicResultSets, short parameterStyle, short sqlAllowed) {

		this(methodName, parameterCount, parameterNames, parameterTypes, parameterModes, 
			dynamicResultSets, parameterStyle, sqlAllowed, true, (TypeDescriptor) null);
	}

	/**
		Create a RoutineAliasInfo for a PROCEDURE or FUNCTION
	*/
	public RoutineAliasInfo(String methodName, int parameterCount, String[] parameterNames,
		TypeDescriptor[]	parameterTypes, int[] parameterModes, int dynamicResultSets, short parameterStyle, short sqlAllowed,
		boolean calledOnNullInput, TypeDescriptor returnType)
	{

		super(methodName);
		this.parameterCount = parameterCount;
		this.parameterNames = parameterNames;
		this.parameterTypes = parameterTypes;
		this.parameterModes = parameterModes;
		this.dynamicResultSets = dynamicResultSets;
		this.parameterStyle = parameterStyle;
		this.sqlAllowed = sqlAllowed;
		this.calledOnNullInput = calledOnNullInput;
		this.returnType = returnType;

		if (SanityManager.DEBUG) {

			if (parameterCount != 0 && parameterNames.length != parameterCount) {
				SanityManager.THROWASSERT("Invalid parameterNames array " + parameterNames.length + " != " + parameterCount);
			}
			else if (parameterCount == 0 && parameterNames != null && parameterNames.length != 0) {
				SanityManager.THROWASSERT("Invalid parameterNames array " + " not zero " + " != " + parameterCount);
			}

			if (parameterCount != 0 && parameterTypes.length != parameterCount) {
				SanityManager.THROWASSERT("Invalid parameterTypes array " + parameterTypes.length + " != " + parameterCount);
			}
			else if (parameterCount == 0 && parameterTypes != null && parameterTypes.length != 0) {
				SanityManager.THROWASSERT("Invalid parameterTypes array " + " not zero " + " != " + parameterCount);
			}

			if (parameterCount != 0 && parameterModes.length != parameterCount) {
				SanityManager.THROWASSERT("Invalid parameterModes array " + parameterModes.length + " != " + parameterCount);
			}
			else if (parameterCount == 0 && parameterModes != null && parameterModes.length != 0) {
				SanityManager.THROWASSERT("Invalid parameterModes array " + " not zero " + " != " + parameterCount);
			}

			if (returnType != null) {
				if (!((sqlAllowed >= RoutineAliasInfo.READS_SQL_DATA) && (sqlAllowed <= RoutineAliasInfo.NO_SQL))) {
					SanityManager.THROWASSERT("Invalid sqlAllowed for FUNCTION " + methodName + " " + sqlAllowed);
				}
			} else {
				if (!((sqlAllowed >= RoutineAliasInfo.MODIFIES_SQL_DATA) && (sqlAllowed <= RoutineAliasInfo.NO_SQL))) {
					SanityManager.THROWASSERT("Invalid sqlAllowed for PROCEDURE " + methodName + " " + sqlAllowed);
				}
				
			}
		}
	}

	public int getParameterCount() {
		return parameterCount;
	}

	public TypeDescriptor[] getParameterTypes() {
		return parameterTypes;
	}

	public int[] getParameterModes() {
		return parameterModes;
	}
	public String[] getParameterNames() {
		return parameterNames;
	}

	public int getMaxDynamicResultSets() {
		return dynamicResultSets;
	}

	public short getParameterStyle() {
		return parameterStyle;
	}

	public short getSQLAllowed() {
		return sqlAllowed;
	}

	public boolean calledOnNullInput() {
		return calledOnNullInput;
	}

	public TypeDescriptor getReturnType() {
		return returnType;
	}


	// Formatable methods

	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
	public void readExternal( ObjectInput in )
		 throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
		specificName = (String) in.readObject();
		dynamicResultSets = in.readInt();
		parameterCount = in.readInt();
		parameterStyle = in.readShort();
		sqlAllowed = in.readShort();
		returnType = (TypeDescriptor) in.readObject();
		calledOnNullInput = in.readBoolean();
		in.readInt(); // future expansion.

		if (parameterCount != 0) {
			parameterNames = new String[parameterCount];
			parameterTypes = new TypeDescriptor[parameterCount];

			ArrayUtil.readArrayItems(in, parameterNames);
			ArrayUtil.readArrayItems(in, parameterTypes);
			parameterModes = ArrayUtil.readIntArray(in);

		} else {
			parameterNames = null;
			parameterTypes = null;
			parameterModes = null;
		}
	}

	/**
	 * Write this object to a stream of stored objects.
	 *
	 * @param out write bytes here.
	 *
	 * @exception IOException		thrown on error
	 */
	public void writeExternal( ObjectOutput out )
		 throws IOException
	{
		super.writeExternal(out);
		out.writeObject(specificName);
		out.writeInt(dynamicResultSets);
		out.writeInt(parameterCount);
		out.writeShort(parameterStyle);
		out.writeShort(sqlAllowed);
		out.writeObject(returnType);
		out.writeBoolean(calledOnNullInput);
		out.writeInt(0); // future expansion
		if (parameterCount != 0) {
			ArrayUtil.writeArrayItems(out, parameterNames);
			ArrayUtil.writeArrayItems(out, parameterTypes);
			ArrayUtil.writeIntArray(out, parameterModes);
		}
	}
 
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.ROUTINE_INFO_V01_ID; }

	public String toString() {

		StringBuffer sb = new StringBuffer(100);
		sb.append(getMethodName());
		sb.append('(');
		for (int i = 0; i < parameterCount; i++) {
			if (i != 0)
				sb.append(',');

			sb.append(RoutineAliasInfo.parameterMode(parameterModes[i]));
			sb.append(' ');
			sb.append(parameterNames[i]);
			sb.append(' ');
			sb.append(parameterTypes[i].getSQLstring());
		}
		sb.append(')');

		sb.append(" LANGUAGE JAVA PARAMETER STYLE JAVA ");
		sb.append(RoutineAliasInfo.SQL_CONTROL[getSQLAllowed()]);
		if (dynamicResultSets != 0) {
			sb.append(" DYNAMIC RESULT SETS ");
			sb.append(dynamicResultSets);
		}

		return sb.toString();
	}

	public static String parameterMode(int parameterMode) {
		switch (parameterMode) {
		case JDBC30Translation.PARAMETER_MODE_IN:
			return "IN";
		case JDBC30Translation.PARAMETER_MODE_OUT:
			return "OUT";
		case JDBC30Translation.PARAMETER_MODE_IN_OUT:
			return "INOUT";
		default:
			return "UNKNOWN";
		}
	}
}
