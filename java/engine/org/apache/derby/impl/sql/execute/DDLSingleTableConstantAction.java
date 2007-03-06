/*

   Derby - Class org.apache.derby.impl.sql.execute.DDLSingleTableConstantAction

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.catalog.UUID;

/**
 * Abstract class that has actions that are across
 * all DDL actions that are tied to a table.  An example
 * of DDL that affects a table is CREATE INDEX or
 * DROP VIEW.  An example of DDL that does not affect
 * a table is CREATE STATEMENT or DROP SCHEMA.
 *
 */
abstract class DDLSingleTableConstantAction extends DDLConstantAction 
{
	protected UUID					tableId;

	
	/**
	 * constructor
	 *
	 * @param tableId the target table
	 */
	DDLSingleTableConstantAction(UUID tableId)
	{
		super();
		this.tableId = tableId;
	}
}
