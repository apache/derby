/*

   Derby - Class org.apache.derby.impl.sql.compile.DMLModGeneratedColumnsStatementNode


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

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SequenceDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.types.RowLocation;

/**
 * A DMLModGeneratedColumnsStatementNode for a table(with identity columns)
 *  modification: to wit, INSERT, UPDATE.
 * The code below used to reside in InsertNode but when we fixed DERBY-6414,
 *  rather than duplicating the code in UpdateNode, we moved the common code 
 *  for insert and update of identity columns to this class.
 *
 */
abstract class DMLModGeneratedColumnsStatementNode extends DMLModStatementNode
{

    protected   RowLocation[] 		autoincRowLocation;

    protected   String              identitySequenceUUIDString;

    DMLModGeneratedColumnsStatementNode
    (
     ResultSetNode resultSet,
     MatchingClauseNode matchingClause,
     int statementType,
     ContextManager cm
    )
    {
        super(resultSet, matchingClause, statementType, cm);
    }

    DMLModGeneratedColumnsStatementNode
    (
     ResultSetNode resultSet,
     MatchingClauseNode matchingClause,
     ContextManager cm
    )
    {
        super(resultSet, matchingClause, cm);
    }

    // if this is 10.11 or higher and the table has an identity column,
    // get the uuid of the sequence generator backing the identity column
    protected String getUUIDofSequenceGenerator() throws StandardException
    {
        DataDictionary dataDictionary = getDataDictionary();
        if (targetTableDescriptor.tableHasAutoincrement() &&
            dataDictionary.checkVersion(DataDictionary.DD_VERSION_DERBY_10_11, null))
        {
            SequenceDescriptor  seq = dataDictionary.getSequenceDescriptor(
                dataDictionary.getSystemSchemaDescriptor(),
                TableDescriptor.makeSequenceName(targetTableDescriptor.getUUID()));
            return (seq.getUUID().toString());
        }
        return null;
    }
}