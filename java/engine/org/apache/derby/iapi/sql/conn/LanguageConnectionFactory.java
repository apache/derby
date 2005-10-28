/*

   Derby - Class org.apache.derby.iapi.sql.conn.LanguageConnectionFactory

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

package org.apache.derby.iapi.sql.conn;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.db.Database;

import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.services.property.PropertyFactory;

import org.apache.derby.iapi.sql.compile.OptimizerFactory;
import org.apache.derby.iapi.sql.compile.NodeFactory;
import org.apache.derby.iapi.sql.compile.CompilerContext;

import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.sql.compile.TypeCompilerFactory;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.Statement;
import org.apache.derby.iapi.sql.compile.Parser;

import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.services.compiler.JavaFactory;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.cache.CacheManager;

import org.apache.derby.iapi.sql.LanguageFactory;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

import java.io.InputStream;

import java.util.Locale;

/**
 * Factory interface for items specific to a connection in the language system.
 * This is expected to be used internally, and so is not in Language.Interface.
 * <p>
 * This Factory provides pointers to other language factories; the
 * LanguageConnectionContext holds more dynamic information, such as
 * prepared statements and whether a commit has occurred or not.
 * <p>
 * This Factory is for internal items used throughout language during a
 * connection. Things that users need for the Database API are in
 * LanguageFactory in Language.Interface.
 * <p>
 * This factory returns (and thus starts) all the other per-database
 * language factories. So there might someday be properties as to which
 * ones to start (attributes, say, like level of optimization).
 * If the request is relative to a specific connection, the connection
 * is passed in. Otherwise, they are assumed to be database-wide services.
 *
 * @see org.apache.derby.iapi.sql.LanguageFactory
 *
 * @author ames
 */
public interface LanguageConnectionFactory {
	/**
		Used to locate this factory by the Monitor basic service.
		There needs to be a language factory per database.
	 */
	String MODULE = "org.apache.derby.iapi.sql.conn.LanguageConnectionFactory";

	/**
		Get a Statement.
		
		@param statementText the text for the statement
		@return	The Statement
	 */
	Statement getStatement(SchemaDescriptor compilationSchema, String statementText);

	/**
		Get a new LanguageConnectionContext. this holds things
		we want to remember about activity in the language system,
		where this factory holds things that are pretty stable,
		like other factories.
		<p>
		The returned LanguageConnectionContext is intended for use
		only by the connection that requested it.

		@return a language connection context for the context stack.
		@exception StandardException the usual
	 */
	LanguageConnectionContext
	newLanguageConnectionContext(ContextManager cm,
								TransactionController tc,
								LanguageFactory lf,
								Database db,
								String userName,
								String drdaID,
								String dbname)

		throws StandardException;

	/**
		Get the UUIDFactory to use with this language connection
	 */
	UUIDFactory	getUUIDFactory();

	/**
		Get the ClassFactory to use with this language connection
	 */
	ClassFactory	getClassFactory();

	/**
		Get the JavaFactory to use with this language connection
	 */
	JavaFactory	getJavaFactory();

	/**
		Get the NodeFactory to use with this language connection
	 */
	NodeFactory	getNodeFactory();

	/**
		Get the ExecutionFactory to use with this language connection
	 */
	ExecutionFactory	getExecutionFactory();

	/**
		Get the PropertyFactory to use with this language connection
	 */
	PropertyFactory	getPropertyFactory();

	/**
		Get the AccessFactory to use with this language connection
	 */
	AccessFactory	getAccessFactory();

	/**
		Get the OptimizerFactory to use with this language connection
	 */
	OptimizerFactory	getOptimizerFactory();

	/**
		Get the TypeCompilerFactory to use with this language connection
	 */
	TypeCompilerFactory getTypeCompilerFactory();

	/**
		Get the DataValueFactory to use with this language connection
		This is expected to get stuffed into the language connection
		context and accessed from there.

	 */
	DataValueFactory		getDataValueFactory(); 

	public CacheManager getStatementCache();

    public Parser newParser(CompilerContext cc);
}
