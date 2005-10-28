/*

   Derby - Class org.apache.derby.vti.IQualifyable

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.vti;

import java.sql.SQLException;
import org.apache.derby.iapi.services.io.Storable;

public interface IQualifyable {

	// public boolean handleQualifier(int relOp, int 

	/**
		Called at runtime before each scan of the VTI.
		The passed in qualifiers are only valid for the single
		execution that follows.
	*/
	public void setQualifiers(VTIEnvironment vtiEnvironment, org.apache.derby.iapi.store.access.Qualifier[][] qualifiers)
		throws SQLException;
}
