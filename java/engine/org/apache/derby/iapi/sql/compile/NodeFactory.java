/*

   Derby - Class org.apache.derby.iapi.sql.compile.NodeFactory

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

package org.apache.derby.iapi.sql.compile;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.error.StandardException;

/* Strictly speaking we shouldn't import classes under the impl hierarchy here
 * but this is work in progress.
 * manish - Wed Mar 28 13:05:19 PST 2001
 */
import	org.apache.derby.impl.sql.compile.QueryTreeNode;

/**
	This is an interface for NodeFactories.
	<p>
	There is expected to be only one of these configured per database.

   @author Rick Hillegas
 */

public abstract class NodeFactory
{
	/**
		Module name for the monitor's module locating system.
	 */
	public static final String MODULE = "org.apache.derby.iapi.sql.compile.NodeFactory";

	/**
	 * Tell whether to do join order optimization.
	 *
	 * @return	Boolean.TRUE means do join order optimization, Boolean.FALSE
	 *			means don't do it.
	 */
	public abstract Boolean	doJoinOrderOptimization();

	/**
	 * Get a node that takes no initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public abstract QueryTreeNode getNode(int nodeType,
							ContextManager cm) throws StandardException;

	/**
	 * Get a node that takes one initializer argument.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	The initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public final QueryTreeNode getNode(int nodeType, Object arg1, ContextManager cm)
													throws StandardException
	{
		QueryTreeNode retval = getNode(nodeType, cm);

		retval.init(arg1);

		return  retval;
	}

	/**
	 * Get a node that takes two initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public final QueryTreeNode getNode(int nodeType,
							Object arg1,
							Object arg2,
							ContextManager cm)
								throws StandardException
	{
		QueryTreeNode retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2);

		return  retval;
	}

	/**
	 * Get a node that takes three initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param arg3	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public final QueryTreeNode getNode(int nodeType,
							Object arg1,
							Object arg2,
							Object arg3,
							ContextManager cm)
								throws StandardException
	{
		QueryTreeNode retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3);

		return  retval;
	}

	/**
	 * Get a node that takes four initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param arg3	An initializer argument
	 * @param arg4	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public QueryTreeNode getNode(int nodeType,
							Object arg1,
							Object arg2,
							Object arg3,
							Object arg4,
							ContextManager cm)
								throws StandardException
	{
		QueryTreeNode retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3, arg4);

		return  retval;
	}


	/**
	 * Get a node that takes five initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param arg3	An initializer argument
	 * @param arg4	An initializer argument
	 * @param arg5	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public QueryTreeNode getNode(int nodeType,
							Object arg1,
							Object arg2,
							Object arg3,
							Object arg4,
							Object arg5,
							ContextManager cm)
								throws StandardException
	{
		QueryTreeNode retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3, arg4, arg5);

		return  retval;
	}

	/**
	 * Get a node that takes six initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param arg3	An initializer argument
	 * @param arg4	An initializer argument
	 * @param arg5	An initializer argument
	 * @param arg6	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public final QueryTreeNode getNode(int nodeType,
							Object arg1,
							Object arg2,
							Object arg3,
							Object arg4,
							Object arg5,
							Object arg6,
							ContextManager cm)
								throws StandardException
	{
		QueryTreeNode retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3, arg4, arg5, arg6);

		return  retval;
	}

	/**
	 * Get a node that takes seven initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param arg3	An initializer argument
	 * @param arg4	An initializer argument
	 * @param arg5	An initializer argument
	 * @param arg6	An initializer argument
	 * @param arg7	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public final QueryTreeNode getNode(int nodeType,
							Object arg1,
							Object arg2,
							Object arg3,
							Object arg4,
							Object arg5,
							Object arg6,
							Object arg7,
							ContextManager cm)
								throws StandardException
	{
		QueryTreeNode retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3, arg4, arg5, arg6, arg7);

		return  retval;
	}
	/**
	 * Get a node that takes eight initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param arg3	An initializer argument
	 * @param arg4	An initializer argument
	 * @param arg5	An initializer argument
	 * @param arg6	An initializer argument
	 * @param arg7	An initializer argument
	 * @param arg8	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public final QueryTreeNode getNode(int nodeType,
							Object arg1,
							Object arg2,
							Object arg3,
							Object arg4,
							Object arg5,
							Object arg6,
							Object arg7,
							Object arg8,
							ContextManager cm)
								throws StandardException
	{
		QueryTreeNode retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8);

		return  retval;
	}
	/**
	 * Get a node that takes nine initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param arg3	An initializer argument
	 * @param arg4	An initializer argument
	 * @param arg5	An initializer argument
	 * @param arg6	An initializer argument
	 * @param arg7	An initializer argument
	 * @param arg8	An initializer argument
	 * @param arg9	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public final QueryTreeNode getNode(int nodeType,
							Object arg1,
							Object arg2,
							Object arg3,
							Object arg4,
							Object arg5,
							Object arg6,
							Object arg7,
							Object arg8,
							Object arg9,
							ContextManager cm)
								throws StandardException
	{
		QueryTreeNode retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9);

		return  retval;
	}
	/**
	 * Get a node that takes ten initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param arg3	An initializer argument
	 * @param arg4	An initializer argument
	 * @param arg5	An initializer argument
	 * @param arg6	An initializer argument
	 * @param arg7	An initializer argument
	 * @param arg8	An initializer argument
	 * @param arg9	An initializer argument
	 * @param arg10	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public final QueryTreeNode getNode(int nodeType,
							Object arg1,
							Object arg2,
							Object arg3,
							Object arg4,
							Object arg5,
							Object arg6,
							Object arg7,
							Object arg8,
							Object arg9,
							Object arg10,
							ContextManager cm)
								throws StandardException
	{
		QueryTreeNode retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
					arg10);

		return  retval;
	}
	/**
	 * Get a node that takes eleven initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param arg3	An initializer argument
	 * @param arg4	An initializer argument
	 * @param arg5	An initializer argument
	 * @param arg6	An initializer argument
	 * @param arg7	An initializer argument
	 * @param arg8	An initializer argument
	 * @param arg9	An initializer argument
	 * @param arg10	An initializer argument
	 * @param arg11	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public final QueryTreeNode getNode(int nodeType,
							Object arg1,
							Object arg2,
							Object arg3,
							Object arg4,
							Object arg5,
							Object arg6,
							Object arg7,
							Object arg8,
							Object arg9,
							Object arg10,
							Object arg11,
							ContextManager cm)
								throws StandardException
	{
		QueryTreeNode retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
					arg10, arg11);

		return  retval;
	}
	/**
	 * Get a node that takes twelve initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param arg3	An initializer argument
	 * @param arg4	An initializer argument
	 * @param arg5	An initializer argument
	 * @param arg6	An initializer argument
	 * @param arg7	An initializer argument
	 * @param arg8	An initializer argument
	 * @param arg9	An initializer argument
	 * @param arg10	An initializer argument
	 * @param arg11	An initializer argument
	 * @param arg12	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public final QueryTreeNode getNode(int nodeType,
							Object arg1,
							Object arg2,
							Object arg3,
							Object arg4,
							Object arg5,
							Object arg6,
							Object arg7,
							Object arg8,
							Object arg9,
							Object arg10,
							Object arg11,
							Object arg12,
							ContextManager cm)
								throws StandardException
	{
		QueryTreeNode retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
					arg10, arg11, arg12);

		return  retval;
	}
	/**
	 * Get a node that takes thirteen initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param arg3	An initializer argument
	 * @param arg4	An initializer argument
	 * @param arg5	An initializer argument
	 * @param arg6	An initializer argument
	 * @param arg7	An initializer argument
	 * @param arg8	An initializer argument
	 * @param arg9	An initializer argument
	 * @param arg10	An initializer argument
	 * @param arg11	An initializer argument
	 * @param arg12	An initializer argument
	 * @param arg13	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public final QueryTreeNode getNode(int nodeType,
							Object arg1,
							Object arg2,
							Object arg3,
							Object arg4,
							Object arg5,
							Object arg6,
							Object arg7,
							Object arg8,
							Object arg9,
							Object arg10,
							Object arg11,
							Object arg12,
							Object arg13,
							ContextManager cm)
								throws StandardException
	{
		QueryTreeNode retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
					arg10, arg11, arg12, arg13);

		return  retval;
	}
	/**
	 * Get a node that takes fourteen initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param arg3	An initializer argument
	 * @param arg4	An initializer argument
	 * @param arg5	An initializer argument
	 * @param arg6	An initializer argument
	 * @param arg7	An initializer argument
	 * @param arg8	An initializer argument
	 * @param arg9	An initializer argument
	 * @param arg10	An initializer argument
	 * @param arg11	An initializer argument
	 * @param arg12	An initializer argument
	 * @param arg13	An initializer argument
	 * @param arg14	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public final QueryTreeNode getNode(int nodeType,
							Object arg1,
							Object arg2,
							Object arg3,
							Object arg4,
							Object arg5,
							Object arg6,
							Object arg7,
							Object arg8,
							Object arg9,
							Object arg10,
							Object arg11,
							Object arg12,
							Object arg13,
							Object arg14,
							ContextManager cm)
								throws StandardException
	{
		QueryTreeNode retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
					arg10, arg11, arg12, arg13, arg14);

		return  retval;
	}

	/**
	 * Get one of the several types of create alias nodes. Carved out of parser
	 * so this could be used by ALTER PUBLICATION.
	 *
	 * @param aliasName				The name of the alias
	 * @param fullStaticMethodName	The full path/method name
	 * @param aliasSpecificInfo	The full path of the target method name,
	 *								if any
	 * @param aliasType	The type of alias to create
	 * @param delimitedIdentifier	Whether or not to treat the class name
	 *								as a delimited identifier if trying to
	 *								resolve it as a class alias.
	 * @param cm			A ContextManager
	 *
	 * @return	A CreateAliasNode matching the given parameters
	 *
	 * @exception StandardException		Thrown on error
	 */
	public abstract QueryTreeNode
	getCreateAliasNode(
		Object aliasName,
		String fullStaticMethodName,
		Object aliasSpecificInfo,
		char aliasType,
		Boolean delimitedIdentifier,
		ContextManager cm)
		throws StandardException;


}

