/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;

/**
 * A NOPStatement node is for statements that don't do anything.  At the
 * time of this writing, the only statements that use it are
 * SET DB2J_DEBUG ON and SET DB2J_DEBUG OFF.  Both of these are
 * executed in the parser, so the statements don't do anything at execution
 */

public class NOPStatementNode extends StatementNode
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	public String statementToString()
	{
		return "NO-OP";
	}

	/**
	 * Bind this NOP statement.  This throws an exception, because NOP
	 * statements by definition stop after parsing.
	 *
	 * @return	The bound query tree
	 *
	 * @exception StandardException		Always thrown to stop after parsing
	 */
	public QueryTreeNode bind() throws StandardException
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
