/*

   Derby - Class org.apache.derby.impl.sql.compile.TriggerReferencingStruct

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.sql.compile;

/**
 * Rudimentary structure for containing information about
 * a REFERENCING clause for a trigger.
 *
 * @author jamie
 */
public class TriggerReferencingStruct 
{
	public String identifier;
	public boolean isRow;
	public boolean isNew;

	public TriggerReferencingStruct
	(
		boolean	isRow, 
		boolean	isNew,
		String	identifier
	)
	{
		this.isRow = isRow;
		this.isNew = isNew;
		this.identifier = identifier;
	}

	public String toString()
	{
		return (isRow ? "ROW " : "TABLE ")+(isNew ? "new: " : "old: ") + identifier;
	}
}
