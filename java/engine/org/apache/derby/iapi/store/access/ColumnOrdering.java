/*

   Derby - Class org.apache.derby.iapi.store.access.ColumnOrdering

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.store.access;

import org.apache.derby.iapi.error.StandardException;


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
}

