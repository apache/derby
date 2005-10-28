/*

   Derby - Class org.apache.derby.impl.store.raw.xact.SavePoint

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

package org.apache.derby.impl.store.raw.xact;

import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.iapi.services.sanity.SanityManager;

class SavePoint
{
	/*
	** Fields
	*/

	private LogInstant savePoint;
	private final String name;
	//kindOfSavepoint can have 3 possible values.
	//A NULL value means it is an internal savepoint (ie not a user defined savepoint)
	//Non NULL value means it is a user defined savepoint which can be a SQL savepoint or a JDBC savepoint
	//     A String value for kindOfSavepoint would mean it is SQL savepoint
	//     A JDBC Savepoint object value for kindOfSavepoint would mean it is JDBC savepoint
	private Object kindOfSavepoint;

	/*
	** Constructor
	*/

	SavePoint(String name, Object kindOfSavepoint) {
		super();
		this.name = name;
		this.kindOfSavepoint = kindOfSavepoint;
	}


	void setSavePoint(LogInstant savePoint) {
        if (SanityManager.DEBUG)
        {
    		SanityManager.ASSERT((savePoint == null) || (this.savePoint == null));
    	}

		this.savePoint = savePoint;
	}

	LogInstant getSavePoint() {
		return savePoint;
	}

	String getName() {
		return name;
	}

	boolean isThisUserDefinedsavepoint() {
		return (kindOfSavepoint != null ? true : false);
	}

	Object getKindOfSavepoint() {
		return kindOfSavepoint;
	}

}
