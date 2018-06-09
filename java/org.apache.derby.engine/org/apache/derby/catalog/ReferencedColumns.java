/*

   Derby - Class org.apache.derby.catalog.ReferencedColumns

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.catalog;


/**
 *	
 *  Provides information about the columns that are referenced by a
 *  CHECK CONSTRAINT definition.
 *  
 *  It is used in the column SYS.SYSCHECKS.REFERENCEDCOLUMNSDESCRIPTOR.
 */
public interface ReferencedColumns
{
	/**
	 * Returns an array of 1-based column positions in the table that the
	 * check constraint is on.  
	 *
	 * @return	An array of ints representing the 1-based column positions
	 *			of the columns that are referenced in this check constraint.
	 */
	public int[]	getReferencedColumnPositions();
	
	/**
	 * Returns an array of 1-based column positions in the trigger table.
	 * These columns are the ones referenced in the trigger action through
	 * the old/new transition variables.
	 *
	 * @return	An array of ints representing the 1-based column positions
	 *			of the columns that are referenced in the trigger action
	 *			through the old/new transition variables.
	 */
	public int[]	getTriggerActionReferencedColumnPositions();
}
