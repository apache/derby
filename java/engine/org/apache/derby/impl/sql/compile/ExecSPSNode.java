/*

   Derby - Class org.apache.derby.impl.sql.compile.ExecSPSNode

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.loader.GeneratedClass;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.compile.CompilerContext;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;

import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.depend.Dependent;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.sql.PreparedStatement;
import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;
import org.apache.derby.impl.sql.execute.BaseActivation;
import org.apache.derby.impl.sql.execute.ColumnInfo;
import org.apache.derby.impl.sql.CursorInfo;

import org.apache.derby.iapi.util.ByteArray;

import java.util.Enumeration;

/**
 * A ExecSPSNode is the root of a QueryTree 
 * that represents an EXECUTE STATEMENT
 * statement.  It is a tad abnormal.  Duringa
 * bind, it locates and retrieves the SPSDescriptor
 * for the particular statement.  At generate time,
 * it generates the prepared statement for the 
 * stored prepared statement and returns it (i.e.
 * it effectively replaces itself with the appropriate
 * prepared statement).
 *
 * @author jamie
 */

public class ExecSPSNode extends StatementNode 
{
	private TableName			name;
	private SPSDescriptor		spsd;
	private ExecPreparedStatement ps;
	private ResultSetNode		usingClause;
	private String				usingText;

	/**
	 * Initializer for a ExecSPSNode
	 *
	 * @param newObjectName		The name of the table to be created
	 * @param usingClause		The using clause
	 * @param usingText			The text of the using clause
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void init(
				Object 		newObjectName,
				Object	usingClause,
				Object			usingText)
		throws StandardException
	{
		this.name = (TableName) newObjectName;
		this.usingClause = (ResultSetNode) usingClause;
		this.usingText = (String) usingText;
	}

	/**
	 * Bind this ExecSPSNode.  This means doing any static error
	 * checking that can be done before actually creating the table.
	 * For example, verifying that the ResultColumnList does not
	 * contain any duplicate column names.
	 *
	 * @return	The bound query tree
	 *
	 * @exception StandardException		Thrown on error
	 */
	public QueryTreeNode bind() throws StandardException
	{
		DataDictionary		dd;

		/*
		** Grab the compiler context each time we bind just
		** to make sure we have the write one (even though
		** we are caching it).
		*/
		dd = getDataDictionary();

		// bind the using Clause
		if (usingClause != null)
		{
			usingClause.bind();
		}

		String schemaName = name.getSchemaName();
		SchemaDescriptor sd = getSchemaDescriptor(name.getSchemaName());
		if (schemaName == null)
			name.setSchemaName(sd.getSchemaName());

		if (sd.getUUID() != null)
			spsd = dd.getSPSDescriptor(name.getTableName(), sd);

		if (spsd == null)
		{
			throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND, "STATEMENT", name);
		}

		if (spsd.getType() == spsd.SPS_TYPE_TRIGGER)
		{
			throw StandardException.newException(SQLState.LANG_TRIGGER_SPS_CANNOT_BE_EXECED, name);
		}
		

		/*
		** This execute statement is dependent on the
		** stored prepared statement.  If for any reason
		** the underlying statement is invalidated by
		** the time we get to execution, the 'execute statement'
		** will get invalidated when the underlying statement
		** is invalidated.
		*/
		getCompilerContext().createDependency(spsd);

		return this;
	}

	/**
	 * SPSes are atomic if its underlying statement is
	 * atomic.
	 *
	 * @return true if the statement is atomic
	 */	
	public boolean isAtomic()
	{

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(ps != null, 
				"statement expected to be bound before calling isAtomic()");
		}

		return ps.isAtomic();
	}

	/**
	 * Do code generation for this statement.  Overrides
	 * the normal generation path in StatementNode.
	 *
	 * @param	ignored - ignored (he he)
	 *
	 * @return		A GeneratedClass for this statement
	 *
	 * @exception StandardException		Thrown on error
	 */
	public GeneratedClass generate(ByteArray ignored) throws StandardException
	{
		//Bug 4821 - commiting the nested transaction will release any bind time locks
		//This way we won't get lock time out errors while trying to update sysstatement
		//table during stale sps recompilation later in the getPreparedstatement() call.
		if (spsd.isValid() == false) {
			getLanguageConnectionContext().commitNestedTransaction();
			getLanguageConnectionContext().beginNestedTransaction(true);
		}  

		/*
		** The following does a prepare on the underlying
		** statement if necessary.  The returned statement
		** is valid and its class is loaded up.
		*/
		ps = spsd.getPreparedStatement();


		/*
		** Set the saved constants from the prepared statement.
		** Put them in the compilation context -- this is where
		** they are expected.
		*/
		getCompilerContext().setSavedObjects(ps.getSavedObjects());
		getCompilerContext().setCursorInfo(ps.getCursorInfo());
		GeneratedClass gc = ps.getActivationClass();

		/*
		** Set up the params for our using clause.
		*/
		setupParams();
		
		return gc;
	}
		
	/**
	 * Make the result description.  Really, we are just
	 * copying it from the stored prepared statement.
	 *
	 * @return	the description
	 */
	public ResultDescription makeResultDescription()
	{
		return ps.getResultDescription();
	}

	/**
	 * Get information about this cursor.  For sps,
	 * this is info saved off of the original query
	 * tree (the one for the underlying query).
	 *
	 * @return	the cursor info
	 */
	public Object getCursorInfo()
	{
		return ps.getCursorInfo();
	}

	/**
	 * Return a description of the ? parameters for the statement
	 * represented by this query tree.  Just return the params
	 * stored with the prepared statement.
	 *
	 * @return	An array of DataTypeDescriptors describing the
	 *		? parameters for this statement.  It returns null
	 *		if there are no parameters.
	 *
	 * @exception StandardException on error
	 */
	public DataTypeDescriptor[]	getParameterTypes() throws StandardException
	{
		return spsd.getParams();
	}


	/**
	 * Create the Constant information that will drive the guts of Execution.
	 * This is assumed to be the first action on this node.
	 *
	 */
	public ConstantAction	makeConstantAction()
	{
		return ps.getConstantAction();
	}

	/**
	 * We need a savepoint if we will do transactional work.
	 * We'll ask the underlying statement if it needs
	 * a savepoint and pass that back.  We have to do this
	 * after generation because getting the PS now might
	 * cause us to basically do DDL (for a stmt recompilation)
	 * which is explicitly banned during binding.  So the
	 * caller can only call this after generate() has retrieved
	 * the target PS.  
	 *
	 * @return boolean	always true.
	 */
	public boolean needsSavepoint()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(ps != null, 
				"statement expected to be bound before calling needsSavepoint()");
		}

		return ps.needsSavepoint();
	}

	/** @see QueryTreeNode#executeStatementName */
	public String executeStatementName()
	{
		return name.getTableName();
	}

	/** @see QueryTreeNode#executeSchemaName */
	public String executeSchemaName()
	{
		return name.getSchemaName();
	}

	/**
	 * Get the name of the SPS that is used
	 * to execute this statement.  Only relevant
	 * for an ExecSPSNode -- otherwise, returns null.
	 *
	 * @return the name of the underlying sps
	 */
	public String getSPSName()
	{
		return spsd.getQualifiedName();
	}
		
	/*
	 * Shouldn't be called
	 */
	int activationKind()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("activationKind not expected "+
				"to be called for a stored prepared statement");
		}
	   return StatementNode.NEED_PARAM_ACTIVATION;
	}
	/////////////////////////////////////////////////////////////////////
	//
	// PRIVATE
	//
	/////////////////////////////////////////////////////////////////////
	/*
	** Set up the parameters for this node.  Takes
	** the usingClause and executes it.  The results
	** are used to set the parameters for the
	** statement.
	*/
	private void setupParams() throws StandardException
	{
		if (usingClause == null)
			return;

		DataTypeDescriptor[] types = spsd.getParams();
		/*
		** If the sps doesn't support any 
		** parameters, then don't bother.
		*/
		if (types == null) 
		{
			return;
		}

		/*
		** Get the results the easy way: create
		** a statement from the text and execute
		** it.
		*/
		LanguageConnectionContext lcc = getLanguageConnectionContext();

		PreparedStatement ps = lcc.prepareInternalStatement(usingText);
	
		ResultSet rs = ps.execute(lcc, false);
		
		try {
			ExecRow row = rs.getNextRow();
			if (row == null)
			{
				throw StandardException.newException(SQLState.LANG_NO_ROWS_FROM_USING);
			}

			/*
			** Get the row and set the parameters based on that
			*/
			ParameterValueSet params = lcc.getLanguageFactory().newParameterValueSet(
				lcc.getLanguageConnectionFactory().getClassFactory().getClassInspector(), types.length, false);

			DataValueDescriptor[] rowArray = row.getRowArray();
			// Check at compile time that the using clause has the correct number of parameters.
			if (rowArray.length != types.length) {
				throw StandardException.newException(SQLState.LANG_NUM_PARAMS_INCORRECT,
					Integer.toString(rowArray.length), Integer.toString(types.length));
			}

			for (int i = 0; i < types.length; i++)
			{
				TypeId typeId = types[i].getTypeId();

				params.setStorableDataValue( 
					typeId.getNull(),
					i, typeId.getJDBCTypeId(), typeId.getCorrespondingJavaTypeName());

				params.getParameterForSet(i).setValue(rowArray[i]);
			}

			/*
			** If there are any other rows, then throw an
			** exception
			*/
			if (rs.getNextRow() != null)
			{
				throw StandardException.newException(SQLState.LANG_USING_CARDINALITY_VIOLATION);
			}
			//bug 4552 - "exec statement using" will return no parameters through parametermetadata
			params.setUsingParameterValueSet();

			/*
			** Stash the parameters in the compiler context
			*/	
			getCompilerContext().setParams(params); 
		}
		finally {

			rs.close();
		}




	}
		
	/////////////////////////////////////////////////////////////////////
	//
	// MISC
	//
	/////////////////////////////////////////////////////////////////////
	public String statementToString()
	{
		return "EXECUTE STATEMENT";
	}

	// called after bind only
	SPSDescriptor getSPSDescriptor()
	{
		return spsd;
	}

	String getUsingText()
	{
		return usingText;
	}
}
