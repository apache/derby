/*

   Derby - Class org.apache.derby.iapi.sql.Statement

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

package org.apache.derby.iapi.sql;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

/**
 * The Statement interface provides a way of giving a statement to the
 * language module, preparing the statement, and executing it. It also
 * provides some support for stored statements. Simple, non-stored,
 * non-parameterized statements can be executed with the execute() method.
 * Parameterized statements must use prepare(). To get the stored query
 * plan for a statement, use get().
 * <p>
 * This interface will have different implementations for the execution-only
 * and compile-and-execute versions of the product. In the execution-only
 * version, some of the methods will do nothing but raise exceptions to
 * indicate that they are not implemented.
 * <p>
 * There is a Statement factory in the Connection interface in the Database
 * module, which uses the one provided in LanguageFactory.
 *
 *	@author Jeff Lichtman
 */
public interface Statement
{

	/**
	 * Generates an execution plan without executing it.
	 *
	 * @return A PreparedStatement that allows execution of the execution
	 *	   plan.
	 * @exception StandardException	Thrown if this is an
	 *	   execution-only version of the module (the prepare() method
	 *	   relies on compilation).
	 */
	PreparedStatement	prepare(LanguageConnectionContext lcc) throws StandardException;

	/**
	 * Generates an execution plan given a set of named parameters.
	 * For generating a storable prepared statement (which
	 * has some extensions over a standard prepared statement).
	 *
	 * @param 	compSchema			the compilation schema to use
	 * @param	paramDefaults		Default parameter values to use for
	 *								optimization
	 * @param	spsSchema schema of the stored prepared statement
	 *
	 * @return A Storable PreparedStatement that allows execution of the execution
	 *	   plan.
	 * @exception StandardException	Thrown if this is an
	 *	   execution-only version of the module (the prepare() method
	 *	   relies on compilation).
	 */
	public	PreparedStatement	prepareStorable
	( 
		LanguageConnectionContext lcc,
		PreparedStatement ps, 
		Object[]			paramDefaults,
		SchemaDescriptor	spsSchema,
		boolean	internalSQL
	)
		throws StandardException;

	/**
	 *	Return the SQL string that this statement is for.
	 *
	 *	@return the SQL string this statement is for.
	 */
	String getSource();

	public boolean getUnicode();

}
