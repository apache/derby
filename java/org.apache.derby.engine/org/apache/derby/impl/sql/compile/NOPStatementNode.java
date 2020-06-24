/*

   Derby - Class org.apache.derby.impl.sql.compile.NOPStatementNode

//IC see: https://issues.apache.org/jira/browse/DERBY-1377
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

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;

/**
 * A NOPStatement node is for statements that don't do anything.  At the
 * time of this writing, the only statements that use it are
 * SET DB2J_DEBUG ON and SET DB2J_DEBUG OFF.  Both of these are
 * executed in the parser, so the statements don't do anything at execution
 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
class NOPStatementNode extends StatementNode
{

    NOPStatementNode(ContextManager cm) {
        super(cm);
    }

    String statementToString()
	{
		return "NO-OP";
	}

	/**
	 * Bind this NOP statement.  This throws an exception, because NOP
	 * statements by definition stop after parsing.
	 *
	 *
	 * @exception StandardException		Always thrown to stop after parsing
	 */
    @Override
	public void bindStatement() throws StandardException
	{
		/*
		** Prevent this statement from getting to execution by throwing
		** an exception during the bind phase.  This way, we don't
		** have to generate a class.
		*/

		throw StandardException.newException(SQLState.LANG_PARSE_ONLY);
	}

	int activationKind()
	{
		   return StatementNode.NEED_NOTHING_ACTIVATION;
	}
}
