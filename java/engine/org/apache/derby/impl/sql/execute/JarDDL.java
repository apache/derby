/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
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
