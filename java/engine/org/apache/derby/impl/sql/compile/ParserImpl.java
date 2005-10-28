/*

   Derby - Class org.apache.derby.impl.sql.compile.ParserImpl

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

package org.apache.derby.impl.sql.compile;

import org.apache.derby.impl.sql.compile.QueryTreeNode;
import org.apache.derby.iapi.sql.compile.Parser;
import org.apache.derby.iapi.sql.Statement;
import org.apache.derby.iapi.sql.compile.CompilerContext;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;

public class ParserImpl implements Parser
{
	/*
	** We will use the following constant to pass in to
	** our CharStream.  It is the size of the internal
	** buffers that are used to buffer tokens.  It
	** should be set to what is typically around the
	** largest token that is likely to be hit.  Note
	** that if the size is exceeded, the buffer will
	** automatically be expanded by 2048, so it is ok
	** to choose something that is smaller than the
	** max token supported.
	**
	** Since, JavaCC generates parser and tokenmanagers classes
	** tightly connected, to use another parser or tokenmanager
	** inherit this class, override the following methods
	** to use specific instances:<ul>
	** <li>getTokenManager()</li>
	** <li>getParser()</li>
	** <li>parseGoalProduction(...)</li>
	** </ul>
	**
	*/
	static final int LARGE_TOKEN_SIZE = 128;

        /* Don't ever access these objects directly, call getParser(), and getTokenManager() */
        protected Object cachedParser; 
	protected Object cachedTokenManager;

	protected CharStream charStream;
        protected String SQLtext;

        protected final CompilerContext cc;

	/**
	 * Constructor for Parser
	 */

	public ParserImpl(CompilerContext cc)
	{
		this.cc = cc;
	}

	public QueryTreeNode parseStatement(String statementSQLText) 
		throws StandardException
	{
		return parseStatement(statementSQLText, (Object[])null);
	}

        /**
	 * Returns a initialized (clean) TokenManager, paired w. the Parser in getParser,
	 * Appropriate for this ParserImpl object.
	 */
        protected Object getTokenManager()
        {
	    /* returned a cached tokenmanager if already exists, otherwise create */
	    SQLParserTokenManager tm = (SQLParserTokenManager) cachedTokenManager;
	    if (tm == null) {
		tm = new SQLParserTokenManager(charStream);
		cachedTokenManager = tm;
	    } else {
		tm.ReInit(charStream);
	    }
	    return tm;
	}

        /**
	 * new parser, appropriate for the ParserImpl object.
	 */
        protected Object getParser()
        {
	    SQLParserTokenManager tm = (SQLParserTokenManager) getTokenManager();
	    /* returned a cached Parser if already exists, otherwise create */
	    SQLParser p = (SQLParser) cachedParser;
	    if (p == null) {
		p = new SQLParser(tm);
		p.setCompilerContext(cc);
		cachedParser = p;
	    } else {
		p.ReInit(tm);
	    }
	    return p;
	}

	/**
	 * Parse a statement and return a query tree.  Implements the Parser
	 * interface
	 *
	 * @param statementSQLTextThe	Statement to parse
	 * @param paramDefaults	parameter defaults. Passed around as an array
	 *                      of objects, but is really an array of StorableDataValues
	 * @param topLevel	true if this is a top level statement, false if not (as in
	 *                            create/copy publication target ddl)*
	 * @return	A QueryTree representing the parsed statement
	 *
	 * @exception StandardException	Thrown on error
	 */

	public QueryTreeNode parseStatement(String statementSQLText, Object[] paramDefaults) 
		throws StandardException
	{

		java.io.Reader sqlText = new java.io.StringReader(statementSQLText);

		/* Get a char stream if we don't have one already */
		if (charStream == null)
		{
			charStream = new UCode_CharStream(sqlText, 1, 1, LARGE_TOKEN_SIZE);
		}
		else
		{
			charStream.ReInit(sqlText, 1, 1, LARGE_TOKEN_SIZE);
		}

		/* remember the string that we're parsing */
		SQLtext = statementSQLText;

		/* Parse the statement, and return the QueryTree */
		try
		{
		    return parseGoalProduction( statementSQLText, paramDefaults);
		}
		catch (ParseException e)
		{
		    throw StandardException.newException(SQLState.LANG_SYNTAX_ERROR, e.getMessage());
		}
		catch (TokenMgrError e)
		{
		    throw StandardException.newException(SQLState.LANG_LEXICAL_ERROR, e.getMessage());
		}
	}

        /**
	 * Parse the goal production, e.g. "statement" for the normal SQL parser.
	 *
	 * @param statementSQLText The Statement to parse
	 * @param paramDefaults	parameter defaults.  Passed around as an array
	 *						of objects, but is really an array of StorableDataValues
         *
	 * @return	A QueryTree representing the parsed statement
	 *
	 * @exception ParseException
         * @exception TokenMgrError
         */
        protected QueryTreeNode parseGoalProduction( String statementSQLText,
                                                   Object[] paramDefaults)
            throws ParseException, TokenMgrError, StandardException
        {
	    SQLParser p = (SQLParser) getParser();
	    return p.Statement( statementSQLText, paramDefaults);
	} // End of parseGoalProduction

	/**
	 * Returns the current SQL text string that is being parsed.
	 *
	 * @return	Current SQL text string.
	 *
	 */
	public	String		getSQLtext()
	{	return	SQLtext; }
}
