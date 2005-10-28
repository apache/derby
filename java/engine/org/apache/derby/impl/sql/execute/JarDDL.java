/*

   Derby - Class org.apache.derby.impl.sql.execute.JarDDL

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecutionContext;

public class JarDDL
{
	/**
	  Add a jar file to the current connection's database.

 	  @exception StandardException Opps
	  */
	static public void
	add(String schemaName, String sqlName, String externalPath)
		 throws StandardException
	{
		schemaName = JarDDL.getSchemaName(schemaName);

		GenericConstantActionFactory caf = getConstantActionFactory();
		ConstantAction ca =
			caf.getAddJarConstantAction(null,schemaName,sqlName,externalPath);
		ca.executeConstantAction(null);
	}


	/**
	  Drop a jar file from the current connection's database.

	  @exception StandardException Opps
	  */
	static public void
	drop(String schemaName, String sqlName)
		 throws StandardException
	{
		schemaName = JarDDL.getSchemaName(schemaName);

		GenericConstantActionFactory caf = getConstantActionFactory();
		ConstantAction ca =
			caf.getDropJarConstantAction(null,schemaName,sqlName);
		ca.executeConstantAction(null);
	}

	/**
	  Replace a jar file from the current connection's database with the content of an
	  external file. 

	  @exception StandardException Opps
	  */
	static public void
	replace(String schemaName, String sqlName,String externalPath)
		 throws StandardException
	{
		schemaName = JarDDL.getSchemaName(schemaName);

		GenericConstantActionFactory caf = getConstantActionFactory();
		ConstantAction ca =
			caf.getReplaceJarConstantAction(null,schemaName,sqlName,externalPath);
		ca.executeConstantAction(null);
	}

	private static GenericConstantActionFactory getConstantActionFactory()
	{
		ExecutionContext ec =
			(ExecutionContext)ContextService.getContext(ExecutionContext.CONTEXT_ID);
		GenericExecutionFactory gef =
			(GenericExecutionFactory)ec.getExecutionFactory();
		GenericConstantActionFactory caf = gef.getConstantActionFactory();
		return caf;
	}

	private static String getSchemaName(String schemaName) {

		if (schemaName != null)
			return schemaName;

        // find the language context.
        LanguageConnectionContext lcc = (LanguageConnectionContext) ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);

		schemaName = lcc.getCurrentSchemaName();

		return schemaName;
	}
	
	/**
	  Make an external name for a file stored in the database.
	  */
	public static String mkExternalName(String schemaName, String sqlName, char separatorChar)
	{
		StringBuffer sb = new StringBuffer(30);

		sb.append("jar");
		sb.append(separatorChar);
		sb.append(schemaName);
		sb.append(separatorChar);
		sb.append(sqlName);
		sb.append(".jar");
		return sb.toString();
	}
}
