/*

   Derby - Class org.apache.derby.impl.sql.compile.ReadCursorNode

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

import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;

/**
 * A ReadCursorNode contains the logic to bind and generate a vanilla
 * SELECT. This node is used for SELECT cursors
 *
 * @author Jeff Lichtman
 */

abstract class ReadCursorNode extends DMLStatementNode
{
	/**
	 * Bind this ReadCursorNode.  This means looking up tables and columns and
	 * getting their types, and figuring out the result types of all
	 * expressions.
	 *
	 * @param	dataDictionary			Namespace to bind against.
	 *
	 * @return	The bound query tree
	 *
	 * @exception StandardException		Thrown on error
	 */

	public QueryTreeNode bind
	(
		DataDictionary 			dataDictionary
    )
		throws StandardException
	{
		FromList	fromList = (FromList) getNodeFactory().getNode(
									C_NodeTypes.FROM_LIST,
									getNodeFactory().doJoinOrderOptimization(),
									getContextManager());

		/* Check for ? parameters directly under the ResultColums */
		resultSet.rejectParameters();

		super.bind(dataDictionary);

		// bind the query expression
		resultSet.bindResultColumns(fromList);

		// this rejects any untyped nulls in the select list
		// pass in null to indicate that we don't have any
		// types for this node
		resultSet.bindUntypedNullsToResultColumns(null);

		/* Verify that all underlying ResultSets reclaimed their FromList */
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(fromList.size() == 0,
				"fromList.size() is expected to be 0, not " + 
				fromList.size() +
				" on return from RS.bindExpressions()");
		}

		return this;
	}

	/**
	 * Do code generation for this ReadCursorNode
	 *
	 * @param acb	The ActivationClassBuilder for the class being built
	 * @param mb	The method the generated code is to go into
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generate(ActivationClassBuilder acb,
								MethodBuilder mb) throws StandardException
	{

		generateAuthorizeCheck(acb, mb,
			org.apache.derby.iapi.sql.conn.Authorizer.SQL_SELECT_OP);

	    // this will generate an expression that will be a ResultSet
        resultSet.generate(acb, mb);
	}

	//////////////////////////////////////////////////////////////////////////////
	//
	// 	QUERY TREE BOILER PLATE
	//
	//////////////////////////////////////////////////////////////////////////////

	public String statementToString() { return "SELECT"; }

}
