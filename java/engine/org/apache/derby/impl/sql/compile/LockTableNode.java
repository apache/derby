/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.compile.CompilerContext;

import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.reference.ClassName;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;
import org.apache.derby.iapi.services.classfile.VMOpcode;

/**
 * A LockTableNode is the root of a QueryTree that represents a LOCK TABLE command:
 *	LOCK TABLE <TableName> IN SHARE/EXCLUSIVE MODE
 *
 * @author Jerry Brenner
 */

public class LockTableNode extends MiscellaneousStatementNode
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
	private TableName	tableName;
	private boolean		exclusiveMode;
	private long		conglomerateNumber;
	private TableDescriptor			lockTableDescriptor;

	/**
	 * Initializer for LockTableNode
	 *
	 * @param tableName		The table to lock
	 * @param exclusiveMode	boolean, whether or not to get an exclusive lock.
	 */
	public void init(Object tableName, Object exclusiveMode)
	{
		this.tableName = (TableName) tableName;
		this.exclusiveMode = ((Boolean) exclusiveMode).booleanValue();
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "tableName: " + tableName + "\n" +
				"exclusiveMode: " + exclusiveMode + "\n" +
				"conglomerateNumber: " + conglomerateNumber + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	public String statementToString()
	{
		return "LOCK TABLE";
	}

	/**
	 * Bind this LockTableNode.  This means looking up the table,
	 * verifying it exists and getting the heap conglomerate number.
	 *
	 * @return	The bound query tree
	 *
	 * @exception StandardException		Thrown on error
	 */

	public QueryTreeNode bind() throws StandardException
	{
		CompilerContext			cc = getCompilerContext();
		ConglomerateDescriptor	cd;
		DataDictionary			dd = getDataDictionary();
		SchemaDescriptor		sd;

		String schemaName = tableName.getSchemaName();
		sd = getSchemaDescriptor(schemaName);

		// Users are not allowed to lock system tables
		if (sd.isSystemSchema())
		{
			throw StandardException.newException(SQLState.LANG_NO_USER_DDL_IN_SYSTEM_SCHEMA, 
							statementToString(), schemaName);
		}

		lockTableDescriptor = getTableDescriptor(tableName.getTableName(), sd);

		if (lockTableDescriptor == null)
		{
			throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND, tableName);
		}

		//throw an exception if user is attempting to lock a temporary table
		if (lockTableDescriptor.getTableType() == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE)
		{
				throw StandardException.newException(SQLState.LANG_NOT_ALLOWED_FOR_DECLARED_GLOBAL_TEMP_TABLE);
		}

		conglomerateNumber = lockTableDescriptor.getHeapConglomerateId();

		/* Get the base conglomerate descriptor */
		cd = lockTableDescriptor.getConglomerateDescriptor(conglomerateNumber);

		/* Statement is dependent on the TableDescriptor and ConglomerateDescriptor */
		cc.createDependency(lockTableDescriptor);
		cc.createDependency(cd);

		return this;
	}

	/**
	 * Return true if the node references SESSION schema tables (temporary or permanent)
	 *
	 * @return	true if references SESSION schema tables, else false
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean referencesSessionSchema()
		throws StandardException
	{
		//If lock table is on a SESSION schema table, then return true. 
		return isSessionSchema(lockTableDescriptor.getSchemaName());
	}

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{
		return getGenericConstantActionFactory().getLockTableConstantAction(
						tableName.getFullTableName(),
						conglomerateNumber,
						exclusiveMode);
	}
}
