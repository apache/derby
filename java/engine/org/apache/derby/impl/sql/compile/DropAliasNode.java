/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
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
