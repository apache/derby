/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.compile
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.compile;

import org.apache.derby.iapi.error.StandardException;

/* Strictly speaking we shouldn't import classes under the impl hierarchy here
 * but this is work in progress.
 * manish - Wed Mar 28 13:05:19 PST 2001
 */
import	org.apache.derby.impl.sql.compile.QueryTreeNode;

/**
 * The Parser interface is intended to work with Jack-generated parsers (now JavaCC).
 * We will specify "STATIC=false" when building Jack parsers - this specifies
 * that the generated classes will not be static, which will allow there to be
 * more than one parser (this is necessary in a multi-threaded server).
 * Non-static parsers do not have to be re-initialized every time they are
 * used (unlike static parsers, for which one must call ReInit() between calls
 * to the parser).
 *
 * @author Jeff Lichtman
 */


public interface Parser
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

	/**
	 * Parses the given statement and returns a query tree. The query tree
	 * at this point is a simple syntactic translation of the statement.
	 * No binding will have taken place, and no decisions will have been
	 * made regarding processing strategy.
	 *
	 * @param statement	The Statement to parse.
	 * @param paramDefaults	Parameter defaults
	 * @return	A new QueryTree representing the syntax of the Statement
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public QueryTreeNode	parseStatement(String statementSQLText,
		Object[] paramDefaults) 
		throws StandardException;


	public QueryTreeNode parseStatement(String statementSQLText)
		throws StandardException;

	/**
	 * Returns the current SQL text string that is being parsed.
	 *
	 * @return	Current SQL text string.
	 *
	 */
	public	String		getSQLtext();

}
