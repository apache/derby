/*

   Derby - Class org.apache.derby.iapi.sql.compile.Parser

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

package org.apache.derby.iapi.sql.compile;

import org.apache.derby.shared.common.error.StandardException;

/**
 * The Parser interface is intended to work with Jack-generated parsers (now JavaCC).
 * We will specify "STATIC=false" when building Jack parsers - this specifies
 * that the generated classes will not be static, which will allow there to be
 * more than one parser (this is necessary in a multi-threaded server).
 * Non-static parsers do not have to be re-initialized every time they are
 * used (unlike static parsers, for which one must call ReInit() between calls
 * to the parser).
 *
 */


public interface Parser
{

	/**
	 * Parses the given statement and returns a query tree. The query tree
	 * at this point is a simple syntactic translation of the statement.
	 * No binding will have taken place, and no decisions will have been
	 * made regarding processing strategy.
	 *
	 * @param statementSQLText	The Statement to parse.
	 * @param paramDefaults	Parameter defaults
	 * @return	A new QueryTree representing the syntax of the Statement
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public Visitable parseStatement(String statementSQLText,
		Object[] paramDefaults) 
		throws StandardException;


	public Visitable parseStatement(String statementSQLText)
		throws StandardException;

    /**
     * Parse an SQL fragment that represents a {@code <search condition>}.
     *
     * @param sqlFragment the SQL fragment to parse
     * @return a parse tree representing the search condition
     * @throws StandardException if the SQL fragment could not be parsed
     */
    public Visitable parseSearchCondition(String sqlFragment)
        throws StandardException;

	/**
	 * Returns the current SQL text string that is being parsed.
	 *
	 * @return	Current SQL text string.
	 *
	 */
	public	String		getSQLtext();

}
