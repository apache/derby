/*

   Derby - Class org.apache.derby.catalog.types.RoutineAliasInfo

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.catalog.types;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.shared.common.util.ArrayUtil;
import org.apache.derby.catalog.TypeDescriptor;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.ParameterMetaData;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.util.IdUtil;

/**
 * Describe a routine (procedure or function) alias.
 *
 * @see org.apache.derby.catalog.AliasInfo
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

	/** PARAMETER STYLE DERBY_JDBC_RESULT_SET */
	public static final short PS_DERBY_JDBC_RESULT_SET = PS_JAVA + 1;

	/** PARAMETER STYLE DERBY */
	public static final short PS_DERBY = PS_DERBY_JDBC_RESULT_SET + 1;

    /** Masks for the sqlOptions field */
    private static final short SQL_ALLOWED_MASK = (short) 0xF;
    private static final short DETERMINISTIC_MASK = (short) 0x10;
    private static final short SECURITY_DEFINER_MASK = (short) 0x20; // Mask for the SECURITY INVOKER/DEFINER field
    private static final short VARARGS_MASK = (short) 0x40;

	private int parameterCount;

    /**
     * Types of the parameters. If there are no parameters
     * then this may be null (or a zero length array).
     */
	private TypeDescriptor[]	parameterTypes;
        /**
         * Name of each parameter. As of DERBY 10.3, parameter names
         * are optional. If the parameter is unnamed, parameterNames[i]
         * is a string of length 0
         */
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
		This field contains several pieces of information:

        bits 0-3    sqlAllowed = MODIFIES_SQL_DATA, READS_SQL_DATA,CONTAINS_SQL, or NO_SQL

        bit 4         on if function is DETERMINISTIC, off otherwise
//IC see: https://issues.apache.org/jira/browse/DERBY-4551
        bit 5         on if running with definer's right, off otherwise
    */
	private short	sqlOptions;

	/**
		SQL Specific name (future)
	*/
	private String	specificName;

	/**
		True if the routine is called on null input.
		(always true for procedures).
	*/
	private boolean	calledOnNullInput;

	// What type of alias is this: PROCEDURE or FUNCTION?
	private transient char aliasType;

	public RoutineAliasInfo() {
	}

	/**
		Create a RoutineAliasInfo for an internal PROCEDURE.
	*/
	public RoutineAliasInfo
        (
         String methodName,
         int parameterCount,
         String[] parameterNames,
         TypeDescriptor[]	parameterTypes,
         int[] parameterModes,
         int dynamicResultSets,
         short parameterStyle,
         short sqlAllowed,
         boolean isDeterministic,
         boolean hasVarargs
         )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-4551
        this(methodName,
             parameterCount,
             parameterNames,
             parameterTypes,
             parameterModes,
             dynamicResultSets,
             parameterStyle,
             sqlAllowed,
             isDeterministic,
             hasVarargs,
             false /* definersRights*/,
             true,
             (TypeDescriptor) null);
	}

	/**
		Create a RoutineAliasInfo for a PROCEDURE or FUNCTION
	*/
    public RoutineAliasInfo(String methodName,
                            int parameterCount,
                            String[] parameterNames,
                            TypeDescriptor[] parameterTypes,
                            int[] parameterModes,
                            int dynamicResultSets,
                            short parameterStyle,
                            short sqlAllowed,
                            boolean isDeterministic,
                            boolean hasVarargs,
                            boolean definersRights,
                            boolean calledOnNullInput,
                            TypeDescriptor returnType)
	{

		super(methodName);
		this.parameterCount = parameterCount;
//IC see: https://issues.apache.org/jira/browse/DERBY-3177
		this.parameterNames = ArrayUtil.copy( parameterNames );
		setParameterTypes( parameterTypes );
		this.parameterModes = ArrayUtil.copy( parameterModes );
		this.dynamicResultSets = dynamicResultSets;
		this.parameterStyle = parameterStyle;
		this.sqlOptions = (short) (sqlAllowed & SQL_ALLOWED_MASK);
        if ( isDeterministic ) { this.sqlOptions = (short) (sqlOptions | DETERMINISTIC_MASK); }
        if ( hasVarargs ) { this.sqlOptions = (short) (sqlOptions | VARARGS_MASK); }

//IC see: https://issues.apache.org/jira/browse/DERBY-4551
        if (definersRights) {
            this.sqlOptions = (short) (sqlOptions | SECURITY_DEFINER_MASK);
        }

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

    /**
     * Types of the parameters. If there are no parameters
     * then this may return null (or a zero length array).
     */
	public TypeDescriptor[] getParameterTypes()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-3177
        return TypeDescriptorImpl.copyTypeDescriptors( parameterTypes );
	}

    /** Set the paramter types. Useful if they need to be bound. */
    public  void    setParameterTypes( TypeDescriptor[] parameterTypes )
    {
		this.parameterTypes = TypeDescriptorImpl.copyTypeDescriptors( parameterTypes );
    }

	public int[] getParameterModes() { return ArrayUtil.copy( parameterModes ); }
        /**
         * Returns an array containing the names of the parameters.
         * As of DERBY 10.3, parameter names are optional (see DERBY-183
         * for more information). If the i-th parameter was unnamed,
         * parameterNames[i] will contain a string of length 0.
         */
	public String[] getParameterNames() { return ArrayUtil.copy( parameterNames );}

	public int getMaxDynamicResultSets() {
		return dynamicResultSets;
	}

	public short getParameterStyle() {
		return parameterStyle;
	}

	public short getSQLAllowed() {
		return (short) (sqlOptions & SQL_ALLOWED_MASK);
	}

    public boolean isDeterministic()
    {
        return ( (sqlOptions & DETERMINISTIC_MASK) != 0 );
    }

    public boolean hasVarargs()
    {
        return ( (sqlOptions & VARARGS_MASK) != 0 );
    }

    public boolean hasDefinersRights()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-4551
        return ( (sqlOptions & SECURITY_DEFINER_MASK) != 0 );
    }

	public boolean calledOnNullInput() {
		return calledOnNullInput;
	}

	public TypeDescriptor getReturnType() {
		return returnType;
	}

	public boolean isTableFunction() {
		if ( returnType == null ) { return false; }
		else { return returnType.isRowMultiSet(); }
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
		sqlOptions = in.readShort();
		returnType = getStoredType(in.readObject());
		calledOnNullInput = in.readBoolean();
		in.readInt(); // future expansion.

		if (parameterCount != 0) {
			parameterNames = new String[parameterCount];
			parameterTypes = new TypeDescriptor[parameterCount];

			ArrayUtil.readArrayItems(in, parameterNames);
            for (int p = 0; p < parameterTypes.length; p++)
            {
                parameterTypes[p] = getStoredType(in.readObject());
            }
			parameterModes = ArrayUtil.readIntArray(in);

		} else {
			parameterNames = null;
			parameterTypes = null;
			parameterModes = null;
		}
	}
    
    /**
     * Old releases (10.3 and before) wrote out the runtime
     * DataTypeDescriptor for routine parameter and return types.
     * 10.4 onwards (DERBY-2775) always writes out the catalog
     * type TypeDescriptor. Here we see what object was read from
     * disk and if it was the old type, now mapped to OldRoutineType,
     * we extract the catalog type and use that.
     * 
     * @param onDiskType The object read that represents the type.
     * @return A type descriptor.
     */
    public static TypeDescriptor getStoredType(Object onDiskType)
    {
        if (onDiskType instanceof OldRoutineType)
            return ((OldRoutineType) onDiskType).getCatalogType();
        return (TypeDescriptor) onDiskType;
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
		out.writeShort(sqlOptions);
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

	/**
	 * Get this alias info as a string.  NOTE: The "ALIASINFO" column
	 * in the SYSALIASES table will return the result of this method
	 * on a ResultSet.getString() call.  That said, since the dblook
	 * utility uses ResultSet.getString() to retrieve ALIASINFO and
	 * to generate the DDL, THIS METHOD MUST RETURN A STRING THAT
	 * IS SYNTACTICALLY VALID, or else the DDL generated by dblook
	 * will be incorrect.
	 */
	public String toString() {

		StringBuffer sb = new StringBuffer(100);
		sb.append(getMethodName());
		sb.append('(');
		for (int i = 0; i < parameterCount; i++) {
			if (i != 0)
				sb.append(',');

//IC see: https://issues.apache.org/jira/browse/DERBY-475
			if (returnType == null) {
			// This is a PROCEDURE.  We only want to print the
			// parameter mode (ex. "IN", "OUT", "INOUT") for procedures--
			// we don't do it for functions since use of the "IN" keyword
			// is not part of the FUNCTION syntax.
//IC see: https://issues.apache.org/jira/browse/DERBY-337
				sb.append(RoutineAliasInfo.parameterMode(parameterModes[i]));
				sb.append(' ');
			}
//IC see: https://issues.apache.org/jira/browse/DERBY-1963
			sb.append(IdUtil.normalToDelimited(parameterNames[i]));
			sb.append(' ');
			sb.append(parameterTypes[i].getSQLstring());
		}
        if ( hasVarargs() ) { sb.append( " ... " ); }
		sb.append(')');

//IC see: https://issues.apache.org/jira/browse/DERBY-475
		if (returnType != null) {
		// this a FUNCTION, so syntax requires us to append the return type.
			sb.append(" RETURNS " + returnType.getSQLstring());
		}

		sb.append(" LANGUAGE JAVA PARAMETER STYLE " );

		switch( parameterStyle )
		{
		    case PS_JAVA:    sb.append( "JAVA " ); break;
		    case PS_DERBY_JDBC_RESULT_SET:    sb.append( "DERBY_JDBC_RESULT_SET " ); break;
		    case PS_DERBY:    sb.append( "DERBY " ); break;
		}
        
//IC see: https://issues.apache.org/jira/browse/DERBY-3570
        if ( isDeterministic() )
        { sb.append( " DETERMINISTIC " ); }

//IC see: https://issues.apache.org/jira/browse/DERBY-4551
        if ( hasDefinersRights())
        { sb.append( " EXTERNAL SECURITY DEFINER " ); }

		sb.append(RoutineAliasInfo.SQL_CONTROL[getSQLAllowed()]);
		if ((returnType == null) &&
			(dynamicResultSets != 0))
		{ // Only print dynamic result sets if this is a PROCEDURE
		  // because it's not valid syntax for FUNCTIONs.
			sb.append(" DYNAMIC RESULT SETS ");
			sb.append(dynamicResultSets);
		}

//IC see: https://issues.apache.org/jira/browse/DERBY-475
		if (returnType != null) {
		// this a FUNCTION, so append the syntax telling what to
		// do with a null parameter.
			sb.append(calledOnNullInput ? " CALLED " : " RETURNS NULL ");
			sb.append("ON NULL INPUT");
		}
		
		return sb.toString();
	}

	public static String parameterMode(int parameterMode) {
		switch (parameterMode) {
//IC see: https://issues.apache.org/jira/browse/DERBY-2438
        case (ParameterMetaData.parameterModeIn):
			return "IN";
        case (ParameterMetaData.parameterModeOut):
			return "OUT";
        case (ParameterMetaData.parameterModeInOut):
			return "INOUT";
		default:
			return "UNKNOWN";
		}
	}
    
    /**
     * Set the collation type of all string types declared for
     * use in this routine to the given collation type.
     * @param collationType
     */
    public void setCollationTypeForAllStringTypes(int collationType)
    {
        if (parameterCount != 0)
        {
            for (int p = 0; p < parameterTypes.length; p++)
                parameterTypes[p] = DataTypeDescriptor.getCatalogType(
                        parameterTypes[p], collationType);
        }
        
        if (returnType != null)
            returnType = DataTypeDescriptor.getCatalogType(returnType, collationType);
    }
}
