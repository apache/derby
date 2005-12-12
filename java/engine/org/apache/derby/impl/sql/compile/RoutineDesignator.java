/*

   Derby - Class org.apache.derby.impl.sql.compile.RoutineDesignator

   Copyright 2005 The Apache Software Foundation or its licensors, as applicable.

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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.impl.sql.execute.PrivilegeInfo;
import org.apache.derby.impl.sql.execute.RoutinePrivilegeInfo;
import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;

import java.util.List;

/**
 * This node represents a routine signature.
 */
public class RoutineDesignator
{
	boolean isSpecific;
	TableName name; // TableName is a misnomer it is really just a schema qualified name
	boolean isFunction; // else a procedure
	/**
	 * A list of DataTypeDescriptors
	 * if null then the signature is not specified and this designator is ambiguous if there is
	 * more than one function (procedure) with this name.
	 */
	List paramTypeList;
	AliasDescriptor aliasDescriptor;

	public RoutineDesignator( boolean isSpecific,
							  TableName name,
							  boolean isFunction,
							  List paramTypeList)
	{
		this.isSpecific = isSpecific;
		this.name = name;
		this.isFunction = isFunction;
		this.paramTypeList = paramTypeList;
	}

	void setAliasDescriptor( AliasDescriptor aliasDescriptor)
	{
		this.aliasDescriptor = aliasDescriptor;
	}
	
	/**
	 * @return PrivilegeInfo for this node
	 */
	PrivilegeInfo makePrivilegeInfo()
	{
		return new RoutinePrivilegeInfo( aliasDescriptor);
	}
}
