/*

   Derby - Class org.apache.derby.impl.sql.compile.DDLStatementNode

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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

/**
 * A DDLStatementNode represents any type of DDL statement: CREATE TABLE,
 * CREATE INDEX, ALTER TABLE, etc.
 *
 */

abstract class DDLStatementNode extends StatementNode
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
	public	static	final	int	UPDATE_STATISTICS = 5;
	public	static	final	int DROP_STATISTICS = 6;


	/////////////////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	/////////////////////////////////////////////////////////////////////////

    private TableName   tableName;
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

    DDLStatementNode(TableName tableName, ContextManager cm) {
        super(cm);
        this.tableName = tableName;
        initOk = true;
    }

    DDLStatementNode(ContextManager cm) {
        super(cm);
    }


	/**
		Initialize the object name we will be performing the DDL
		on and check that we are not in the system schema
		and that DDL is allowed.
	*/
	protected void initAndCheck(Object objectName)
		throws StandardException {

        this.tableName = (TableName) objectName;

		initOk = true;
	}

	/**
	 * A DDL statement is always atomic
	 *
	 * @return true 
	 */	
    @Override
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
    String getRelativeName()
	{
        return tableName.getTableName() ;
	}

	/**
	 * Return the full dot expression name of the 
	 * object being dropped.
	 * 
	 * @return the full name
	 */
    String getFullName()
	{
        return tableName.getFullTableName() ;
	}

    public  final TableName getObjectName() { return tableName; }

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */
    @Override
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
            return ((tableName==null)?"":
                    "name: " + tableName.toString() +"\n") + super.toString();
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
	 * @exception StandardException		Thrown on error
	 */
    @Override
    final void generate(ActivationClassBuilder acb, MethodBuilder mb)
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
	* Checks if current authorizationID is owner of the schema.
	*
	* @return Schema Descriptor
	*
	* @exception	StandardException	throws on schema name
	*						that doesn't exist	
	*/
	protected final SchemaDescriptor getSchemaDescriptor() throws StandardException
	{
		return getSchemaDescriptor(true, true);
	}

	/**
	* Get a schema descriptor for this DDL object.
	* Uses this.objectName.  Always returns a schema,
	* we lock in the schema name prior to execution.
	* 
	* The most common call to this method is with 2nd 
	* parameter true which says that SchemaDescriptor
	* should not be requested for system schema. The only
	* time this method will get called with 2nd parameter
	* set to false will be when user has requested for
	* inplace compress using 
	* SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE
	* Above inplace compress can be invoked on system tables.
	* A call to SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE 
	* internally gets translated into ALTER TABLE sql.
	* When ALTER TABLE is executed for SYSCS_INPLACE_COMPRESS_TABLE,
	* we want to allow SchemaDescriptor request for system
	* tables. DERBY-1062
	*
	* @param ownerCheck		If check for schema owner is needed
	* @param doSystemSchemaCheck   If check for system schema is needed.
	*    If set to true, then throw an exception if schema descriptor
	*    is requested for a system schema. The only time this param 
	*    will be set to false is when user is asking for inplace
	*    compress of a system table. DERBY-1062
	*
	* @return Schema Descriptor
	*
	* @exception	StandardException	throws on schema name
	*						that doesn't exist	
	*/
	protected final SchemaDescriptor getSchemaDescriptor(boolean ownerCheck,
			boolean doSystemSchemaCheck)
		 throws StandardException
	{
        String schemaName = tableName.getSchemaName();
		//boolean needError = !(implicitCreateSchema || (schemaName == null));
		boolean needError = !implicitCreateSchema;
		SchemaDescriptor sd = getSchemaDescriptor(schemaName, needError);
		CompilerContext cc = getCompilerContext();

		if (sd == null) {
			/* Disable creating schemas starting with SYS */
			if (schemaName.startsWith("SYS"))
				throw StandardException.newException(
					SQLState.LANG_NO_USER_DDL_IN_SYSTEM_SCHEMA,
					statementToString(),
					schemaName);

			sd  = new SchemaDescriptor(getDataDictionary(), schemaName,
				(String) null, (UUID)null, false);

			if (isPrivilegeCollectionRequired())
				cc.addRequiredSchemaPriv(schemaName, null, Authorizer.CREATE_SCHEMA_PRIV);
		}

		if (ownerCheck && isPrivilegeCollectionRequired())
			cc.addRequiredSchemaPriv(sd.getSchemaName(), null,
						Authorizer.MODIFY_SCHEMA_PRIV);

		/*
		** Catch the system schema here if the caller wants us to.
		** Currently, the only time we allow system schema is for inplace
		** compress table calls.
		*/	 
		if (doSystemSchemaCheck && sd.isSystemSchema())
		{
			throw StandardException.newException(SQLState.LANG_NO_USER_DDL_IN_SYSTEM_SCHEMA,
							statementToString(), sd);
		}
		return sd;
	}

	protected final TableDescriptor getTableDescriptor()
		throws StandardException
	{
        return getTableDescriptor(tableName);
	}

	/**
	 * Validate that the table is ok for DDL -- e.g.
	 * that it exists, it is not a view. It is ok for
	 * it to be a system table. Also check that its 
	 * schema is ok. Currently, the only time this method
	 * is called is when user has asked for inplace 
	 * compress. eg
	 * call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE('SYS','SYSTABLES',1,1,1);
	 * Inplace compress is allowed on both system and
	 * user tables.
	 *
	 * @return the validated table descriptor, never null
	 *
	 * @exception StandardException on error
	 */
	protected final TableDescriptor getTableDescriptor(boolean doSystemTableCheck)
	throws StandardException
	{
        TableDescriptor td = justGetDescriptor(tableName);
		td = checkTableDescriptor(td,doSystemTableCheck);
		return td;
	}

	protected final TableDescriptor getTableDescriptor(UUID tableId)
		throws StandardException {

		TableDescriptor td = getDataDictionary().getTableDescriptor(tableId);

		td = checkTableDescriptor(td,true);
		return td;

	}

	/**
	 * Validate that the table is ok for DDL -- e.g.
	 * that it exists, it is not a view, and is not
	 * a system table, and that its schema is ok.
	 *
	 * @return the validated table descriptor, never null
	 *
	 * @exception StandardException on error
	 */
	protected final TableDescriptor getTableDescriptor(TableName tableName)
		throws StandardException
	{
		TableDescriptor td = justGetDescriptor(tableName);

		/* beetle 4444, td may have changed when we obtain shared lock */
		td = checkTableDescriptor(td, true);
		return td;

	}

	/**
	 * Just get the table descriptor. Don't worry if it belongs to a view,
	 * system table, synonym or a real table. Let the caller decide what
	 * to do.
	 * 
	 * @param tableName
	 * 
	 * @return TableDescriptor for the give TableName
	 * 
	 * @throws StandardException on error
	 */
	private TableDescriptor justGetDescriptor(TableName tableName)
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
		return td;
	}

	private TableDescriptor checkTableDescriptor(TableDescriptor td, 
			boolean doSystemTableCheck)
		throws StandardException
	{
		String sqlState = null;

		switch (td.getTableType()) {
		case TableDescriptor.VTI_TYPE:
			sqlState = SQLState.LANG_INVALID_OPERATION_ON_SYSTEM_TABLE;
			break;

		case TableDescriptor.SYSTEM_TABLE_TYPE:
			if (doSystemTableCheck)
				/*
				** Not on system tables (though there are no constraints on
				** system tables as of the time this is writen
				*/
				sqlState = SQLState.LANG_INVALID_OPERATION_ON_SYSTEM_TABLE;
			else
				//allow system table. The only time this happens currently is
				//when user is requesting inplace compress on system table
				return td;
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
	  *	Make a from list for binding query fragments in a CREATE/ALTER TABLE
      *     statement.
      *
      * @param dd  Metadata.
      * @param tableElementList the parenthesized list of columns and constraints in a CREATE/ALTER TABLE statement
      * @param creatingTable true if this is for CREATE TABLE. false if this is for ALTER TABLE
      */
	FromList	makeFromList( DataDictionary dd, TableElementList tableElementList, boolean creatingTable )
        throws StandardException
	{
        // DERBY-3043: To avoid a no-such-schema error when
        // binding the check constraint, ensure that the
        // table we bind against has a schema name specified.
        // If it doesn't, fill in the schema name now.
        //
        TableName tableName = getObjectName();
        if (tableName.getSchemaName() == null)
        { tableName.setSchemaName(getSchemaDescriptor().getSchemaName()); }
        
        FromList fromList = new FromList(
                getOptimizerFactory().doJoinOrderOptimization(),
                getContextManager());

        FromBaseTable table = new FromBaseTable(
             tableName,
             null,
             null,
             null,
             getContextManager()
             );

        if ( creatingTable )
        {
            table.setTableNumber(0);
			fromList.addFromTable(table);
            table.setResultColumns(new ResultColumnList(getContextManager()));
        }
        else // ALTER TABLE
        {
            fromList.addFromTable(table);
            fromList.bindTables(
                 dd,
                 new FromList(
                    getOptimizerFactory().doJoinOrderOptimization(),
                    getContextManager()));
        }
        
        tableElementList.appendNewColumnsToRCL(table);

        return fromList;
	}
    
    @Override
    void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);

        if (tableName != null) {
            tableName = (TableName) tableName.accept(v);
        }
    }
}
