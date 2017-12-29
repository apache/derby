/*

   Derby - Class org.apache.derby.iapi.store.access.ColumnOrdering

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

package org.apache.derby.iapi.store.access;

import org.apache.derby.shared.common.error.StandardException;


/**

  The column ordering interface defines a column that is to be
  ordered in a sort or index, and how it is to be ordered.  Column
  instances are compared by calling the compare(Orderable) method
  of Orderable.

**/

public interface ColumnOrdering
{
	int getColumnId();
	boolean getIsAscending();
        /**
         * Indicate whether NULL values should be ordered below non-NULL.
         *
         * This function returns TRUE if the user has specified, via the
         * <null ordering> clause in the ORDER BY clause, that NULL values
         * of this column should sort lower than non-NULL values.
         *
         * @return whether nulls should sort low
         */
	public boolean getIsNullsOrderedLow();
}

