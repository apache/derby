/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * A DropTriggerNode is the root of a QueryTree that represents a DROP TRIGGER
 * statement.
 *
 * @author Jamie
 */
public class DropTriggerNode extends DropStatementNode
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
	private TableDescriptor td;

	public String statementToString()
	{
		return "DROP TRIGGER";
	}

	/**
	 * Bind this DropTriggerNode.  This means looking up the trigger,
	 * verifying it exists and getting its table uuid.
	 *
	 * @return	The bound query tree
	 *
	 * @exception StandardException		Thrown on error
	 */
	public QueryTreeNode bind() throws StandardException
	{
		CompilerContext			cc = getCompilerContext();
		DataDictionary			dd = getDataDictionary();

		SchemaDescriptor sd = getSchemaDescriptor();

		TriggerDescriptor triggerDescriptor = null;
		
		if (sd.getUUID() != null)
			triggerDescriptor = dd.getTriggerDescriptor(getRelativeName(), sd);

		if (triggerDescriptor == null)
		{
			throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND, "TRIGGER", getFullName());
		}

		/* Get the table descriptor */
		td = triggerDescriptor.getTableDescriptor();
		cc.createDependency(td);
		cc.createDependency(triggerDescriptor);
			
		return this;
	}

	// inherit generate() method from DDLStatementNode

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{
		return	getGenericConstantActionFactory().getDropTriggerConstantAction(
										 	getSchemaDescriptor(),
											getRelativeName(),
											td.getUUID());
	}
}
