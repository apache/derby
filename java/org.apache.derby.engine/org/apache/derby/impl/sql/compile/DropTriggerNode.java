/*

   Derby - Class org.apache.derby.impl.sql.compile.DropTriggerNode

//IC see: https://issues.apache.org/jira/browse/DERBY-1377
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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;

/**
 * A DropTriggerNode is the root of a QueryTree that represents a DROP TRIGGER
 * statement.
 *
 */
class DropTriggerNode extends DDLStatementNode
{
	private TableDescriptor td;

    DropTriggerNode(TableName trigger, ContextManager cm) {
        super(trigger, cm);
    }

    String statementToString()
	{
		return "DROP TRIGGER";
	}

	/**
	 * Bind this DropTriggerNode.  This means looking up the trigger,
	 * verifying it exists and getting its table uuid.
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public void bindStatement() throws StandardException
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
	}

	// inherit generate() method from DDLStatementNode

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
    @Override
    public ConstantAction makeConstantAction() throws StandardException
	{
		return	getGenericConstantActionFactory().getDropTriggerConstantAction(
										 	getSchemaDescriptor(),
											getRelativeName(),
											td.getUUID());
	}
}
