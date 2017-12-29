/*

   Derby - Class org.apache.derby.impl.sql.compile.ParserImpl

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

package org.apache.derby.impl.sql.compile;

import java.io.StringReader;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.Parser;
import org.apache.derby.iapi.sql.compile.Visitable;

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
        private SQLParser cachedParser; 
        private Object cachedTokenManager;

        private CharStream charStream;
        protected String SQLtext;

        private final CompilerContext cc;

	/**
	 * Constructor for Parser
	 */

	public ParserImpl(CompilerContext cc)
	{
		this.cc = cc;
	}

	public Visitable parseStatement(String statementSQLText)
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
     private SQLParser getParser()
        {
	    SQLParserTokenManager tm = (SQLParserTokenManager) getTokenManager();
	    /* returned a cached Parser if already exists, otherwise create */
        SQLParser p = cachedParser;
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
	 * @param statementSQLText	Statement to parse
	 * @param paramDefaults	parameter defaults. Passed around as an array
	 *                      of objects, but is really an array of StorableDataValues
	 * @return	A QueryTree representing the parsed statement
	 *
	 * @exception StandardException	Thrown on error
	 */

	public Visitable parseStatement(String statementSQLText, Object[] paramDefaults)
		throws StandardException
	{
        return parseStatementOrSearchCondition(
                statementSQLText, paramDefaults, true);
    }

    /**
     * Parse a full SQL statement or a fragment that represents a
     * {@code <search condition>}.
     *
     * @param sql the SQL statement or fragment to parse
     * @param paramDefaults parameter defaults to pass on to the parser
     *   in the case where {@code sql} is a full SQL statement
     * @param isStatement {@code true} if {@code sql} is a full SQL statement,
     *   {@code false} if it is a fragment
     * @return parse tree for the SQL
     * @throws StandardException if an error happens during parsing
     */
    private Visitable parseStatementOrSearchCondition(
            String sql, Object[] paramDefaults, boolean isStatement)
        throws StandardException
    {
        StringReader sqlText = new StringReader(sql);

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
        SQLtext = sql;

		/* Parse the statement, and return the QueryTree */
		try
		{
            SQLParser p = getParser();
            return isStatement
                    ? p.Statement(sql, paramDefaults)
                    : p.SearchCondition(sql);
		}
		catch (ParseException e)
		{
		    throw StandardException.newException(SQLState.LANG_SYNTAX_ERROR, e.getMessage());
		}
		catch (TokenMgrError e)
		{
			// Derby - 2103.
			// When the exception occurs cachedParser may live with
			// some flags set inappropriately that may cause Exception
			// in the subsequent compilation. This seems to be a javacc bug.
			// Issue Javacc-152 has been raised.
			// As a workaround, the cachedParser object is cleared to ensure
			// that the exception does not have any side effect.
			// TODO : Remove the following line if javacc-152 is fixed.
			cachedParser = null;
		    throw StandardException.newException(SQLState.LANG_LEXICAL_ERROR, e.getMessage());
		}
	}

    @Override
    public Visitable parseSearchCondition(String sqlFragment)
            throws StandardException {
        return parseStatementOrSearchCondition(sqlFragment, null, false);
    }

	/**
	 * Returns the current SQL text string that is being parsed.
	 *
	 * @return	Current SQL text string.
	 *
	 */
	public	String		getSQLtext()
	{	return	SQLtext; }
}
