/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;
import org.apache.derby.iapi.services.compiler.JavaFactory;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;

import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * A DropSPSNode is the root of a QueryTree that represents a DROP STATEMENT
 * statement.
 *
 * @author Jamie
 */

public class DropSPSNode extends DropStatementNode
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
	public String statementToString()
	{
		return "DROP STATEMENT";
	}

	// inherit generate() method from DDLStatementNode
	/**
	 * Bind this DropSPSNode.  
	 *
	 * @return	The bound query tree
	 *
	 * @exception StandardException		Thrown on error
	 */
	public QueryTreeNode bind() throws StandardException
	{
		super.bind();
		
		DataDictionary			dd = getDataDictionary();
		SchemaDescriptor sd = getSchemaDescriptor();

		SPSDescriptor spsd = null;
		
		if (sd.getUUID() != null)
			spsd = dd.getSPSDescriptor(getRelativeName(), sd);

		if (spsd == null)
		{
			throw StandardException.newException(SQLState.LANG_OBJECT_DOES_NOT_EXIST, "DROP STATEMENT", getFullName());
		}
		
		if (spsd.getType() == spsd.SPS_TYPE_TRIGGER)
		{
			throw StandardException.newException(SQLState.LANG_CANNOT_DROP_TRIGGER_S_P_S, getFullName());
		}

		return this;
	}

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{
		return	getGenericConstantActionFactory().getDropSPSConstantAction(
										 	getSchemaDescriptor(),
											getRelativeName());
	}
}
