/*

   Derby - Class org.apache.derby.iapi.sql.execute.ConstantAction

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

package org.apache.derby.iapi.sql.execute;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.catalog.UUID;

/**
 *	This interface describes actions that are ALWAYS performed for a
 *	Statement at Execution time. For instance, it is used for DDL
 *	statements to describe what they should stuff into the catalogs.
 *	<p>
 *	An object satisfying this interface is put into the PreparedStatement
 *	and run at Execution time. Thus ConstantActions may be shared
 *  across threads and must not store connection/thread specific
 *  information in any instance field.
 *
 */

public interface ConstantAction
{
    /** clauseType for WHEN NOT MATCHED ... THEN INSERT */
    public  static  final   int WHEN_NOT_MATCHED_THEN_INSERT = 0;
    /** clauseType for WHEN MATCHED ... THEN UPDATE */
    public  static  final   int WHEN_MATCHED_THEN_UPDATE = 1;
    /** clauseType for WHEN MATCHED ... THEN DELETE */
    public  static  final   int WHEN_MATCHED_THEN_DELETE = 2;

	/**
	 *	Run the ConstantAction.
	 *
	 * @param	activation	The execution environment for this constant action.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException;
}
