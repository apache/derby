/*

   Derby - Class org.apache.derby.impl.sql.compile.DropAliasNode

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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

import org.apache.derby.catalog.AliasInfo;

/**
 * A DropAliasNode  represents a DROP ALIAS statement.
 *
 * @author Jerry Brenner
 */

public class DropAliasNode extends DropStatementNode
{
	private char aliasType;
	private char nameSpace;

	/**
	 * Initializer for a DropAliasNode
	 *
	 * @param dropAliasName	The name of the method alias being dropped
	 * @param aliasType				Alias type
	 *
	 * @exception StandardException
	 */
	public void init(Object dropAliasName, Object aliasType)
				throws StandardException
	{
		TableName dropItem = (TableName) dropAliasName;
		initAndCheck(dropItem);
		this.aliasType = ((Character) aliasType).charValue();
	
		switch (this.aliasType)
		{
			case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
				nameSpace = AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR;
				break;

			case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
				nameSpace = AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR;
				break;

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("bad type to DropAliasNode: "+this.aliasType);
				}
		}
	}

	public	char	getAliasType() { return aliasType; }

	public String statementToString()
	{
		return "DROP ".concat(aliasTypeName(aliasType));
	}

	/**
	 * Bind this DropMethodAliasNode.  
	 *
	 * @return	The bound query tree
	 *
	 * @exception StandardException		Thrown on error
	 */
	public QueryTreeNode bind() throws StandardException
	{
		DataDictionary	dataDictionary = getDataDictionary();
		String			aliasName = getRelativeName();

		AliasDescriptor	ad = null;
		SchemaDescriptor sd = getSchemaDescriptor();
		
		if (sd.getUUID() != null) {
			ad = dataDictionary.getAliasDescriptor
			                          (sd.getUUID().toString(), aliasName, nameSpace );
		}
		if ( ad == null )
		{
			throw StandardException.newException(SQLState.LANG_OBJECT_DOES_NOT_EXIST, statementToString(), aliasName);
		}

		// User cannot drop a system alias
		if (ad.getSystemAlias())
		{
			throw StandardException.newException(SQLState.LANG_CANNOT_DROP_SYSTEM_ALIASES, aliasName);
		}

		return this;
	}

	// inherit generate() method from DDLStatementNode


	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{
		return	getGenericConstantActionFactory().getDropAliasConstantAction(getSchemaDescriptor(), getRelativeName(), nameSpace);
	}

	/* returns the alias type name given the alias char type */
	private static String aliasTypeName( char actualType)
	{
		String	typeName = null;

		switch ( actualType )
		{
			case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
				typeName = "PROCEDURE";
				break;
			case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
				typeName = "FUNCTION";
				break;
		}
		return typeName;
	}
}
