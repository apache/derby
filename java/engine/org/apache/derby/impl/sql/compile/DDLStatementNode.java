/*

   Derby - Class org.apache.derby.impl.sql.compile.DDLStatementNode

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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.reference.ClassName;

import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.catalog.UUID;

/**
 * A DDLStatementNode represents any type of DDL statement: CREATE TABLE,
 * CREATE INDEX, ALTER TABLE, etc.
 *
 * @author Jeff Lichtman
 */

public abstract class DDLStatementNode extends StatementNode
{
	/////////////////////////////////////////////////////////////////////////
	//
	//	CONSTANTS
	//
	/////////////////////////////////////////////////////////////////////////

	public	static	final	int	UNKNOWN_TYPE = 0;
	public	static	final	int	ADD_TYPE = 1;
	public	static	final	int	DROP_TYPE = 2;
	public	static	final	int	MODIFY_TYPE = 3;
	public	static	final	int	LOCKING_TYPE = 4;


	/////////////////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	/////////////////////////////////////////////////////////////////////////

	private TableName	objectName;
	private boolean		initOk;

	/**
		sub-classes can set this to be true to allow implicit
		creation of the main object's schema at execution time.
	*/
	boolean implicitCreateSchema;


	/////////////////////////////////////////////////////////////////////////
	//
	//	BEHAVIOR
	//
	/////////////////////////////////////////////////////////////////////////

	public void init(Object objectName)
		throws StandardException {
		initAndCheck(objectName);
	}

	/**
		Initialize the object name we will be performing the DDL
		on and check that we are not in the system schema
		and that DDL is allowed.
	*/
	protected void initAndCheck(Object objectName)
		throws StandardException {

		this.objectName = (TableName) objectName;

		initOk = true;
	}

	/**
	 * A DDL statement is always atomic
	 *
	 * @return true 
	 */	
	public boolean isAtomic()
	{
		return true;
	}

	/**
	 * Return the name of the table being dropped.
	 * This is the unqualified table name.
	 *
	 * @return the relative name
	 */
	public String getRelativeName()
	{
		return objectName.getTableName() ;
	}

	/**
	 * Return the full dot expression name of the 
	 * object being dropped.
	 * 
	 * @return the full name
	 */
	public String getFullName()
	{
		return objectName.getFullTableName() ;
	}

    public	final TableName	getObjectName() { return objectName; }

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
			return objectName.toString() + super.toString();
		}
		else
		{
			return "";
		}
	}

	int activationKind()
	{
		   return StatementNode.NEED_DDL_ACTIVATION;
	}

	/**
	 * Generic generate code for all DDL statements.
	 *
	 * @param acb	The ActivationClassBuilder for the class being built
	 * @param mb	The execute() method to be built
	 *
	 * @return		A compiled expression returning the RepCreatePublicationResultSet
	 *
	 * @exception StandardException		Thrown on error
	 */

	public final void generate(ActivationClassBuilder acb,
								MethodBuilder mb)
							throws StandardException
	{
		if (SanityManager.DEBUG) {
			if (!initOk)
				SanityManager.THROWASSERT(getClass() + " never called initAndCheck()");
		}

		// The generated java is the expression:
		// return ResultSetFactory.getDDLResultSet(this)
		//		                       

		acb.pushGetResultSetFactoryExpression(mb); // instance for getDDLResultSet
		acb.pushThisAsActivation(mb); // first arg

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getDDLResultSet", ClassName.ResultSet, 1);
	}

	 

	/**
	* Get a schema descriptor for this DDL object.
	* Uses this.objectName.  Always returns a schema,
	* we lock in the schema name prior to execution.
	*
	* @return Schema Descriptor
	*
	* @exception	StandardException	throws on schema name
	*						that doesn't exist	
	*/
	protected final SchemaDescriptor getSchemaDescriptor() throws StandardException
	{
		String schemaName = objectName.getSchemaName();
		//boolean needError = !(implicitCreateSchema || (schemaName == null));
		boolean needError = !implicitCreateSchema;
		SchemaDescriptor sd = getSchemaDescriptor(schemaName, needError);

		if (sd == null) {
			/* Disable creating schemas starting with SYS */
			if (schemaName.startsWith("SYS"))
				throw StandardException.newException(SQLState.LANG_NO_USER_DDL_IN_SYSTEM_SCHEMA,
					statementToString(), schemaName);

			sd  = new SchemaDescriptor(getDataDictionary(), schemaName,
				(String) null, (UUID)null, false);
		}

		/*
		** Catch the system schema here.
		*/	 
		if (sd.isSystemSchema())
		{
			throw StandardException.newException(SQLState.LANG_NO_USER_DDL_IN_SYSTEM_SCHEMA,
							statementToString(), sd);
		}
		return sd;
	}

	protected final TableDescriptor getTableDescriptor()
		throws StandardException
	{
		return getTableDescriptor(objectName);
	}

	protected final TableDescriptor getTableDescriptor(UUID tableId)
		throws StandardException {

		TableDescriptor td = getDataDictionary().getTableDescriptor(tableId);

		td = checkTableDescriptor(td);
		return td;

	}


	/**
	 * Validate that the table is ok for DDL -- e.g.
	 * that it exists, it is not a view, and is not
	 * a system table, and that its schema is ok.
	 *
	 * @param tableDescriptor td
	 *
	 * @return the validated table descriptor, never null
	 *
	 * @exception StandardException on error
	 */
	protected final TableDescriptor getTableDescriptor(TableName tableName)
		throws StandardException
	{
		String schemaName = tableName.getSchemaName();
		SchemaDescriptor sd = getSchemaDescriptor(schemaName);
		
		TableDescriptor td = getTableDescriptor(tableName.getTableName(), sd);

		if (td == null)
		{
			throw StandardException.newException(SQLState.LANG_OBJECT_DOES_NOT_EXIST, 
						statementToString(), tableName);
		}

		/* beetle 4444, td may have changed when we obtain shared lock */
		td = checkTableDescriptor(td);
		return td;

	}

	private TableDescriptor checkTableDescriptor(TableDescriptor td)
		throws StandardException
	{
		String sqlState = null;

		switch (td.getTableType()) {
		case TableDescriptor.SYSTEM_TABLE_TYPE:

			/*
			** Not on system tables (though there are no constraints on
			** system tables as of the time this is writen
			*/
			sqlState = SQLState.LANG_INVALID_OPERATION_ON_SYSTEM_TABLE;
			break;

		case TableDescriptor.BASE_TABLE_TYPE:
			/* need to IX lock table if we are a reader in DDL datadictionary
			 * cache mode, otherwise we may interfere with another DDL thread
			 * that is in execution phase; beetle 4343, also see $WS/docs/
			 * language/SolutionsToConcurrencyIssues.txt (point f)
			 */
			return lockTableForCompilation(td);

		case TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE:
			return td;

		/*
		** Make sure it is not a view
		*/
		case TableDescriptor.VIEW_TYPE:
			sqlState = SQLState.LANG_INVALID_OPERATION_ON_VIEW;
			break;
		}

		
		throw StandardException.newException(sqlState, 
				statementToString(), td.getQualifiedName());

	}

	/**
	  *	Bind the  object Name. This means filling in the schema name if it
	  *	wasn't specified.
	  *
	  *	@param	dataDictionary	Data dictionary to bind against.
	  *
	  *	@exception StandardException		Thrown on error
	  */
	public	void	bindName( DataDictionary	dataDictionary )
		                       throws StandardException
	{
		if (objectName != null)
			objectName.bind( dataDictionary );
	}
}
