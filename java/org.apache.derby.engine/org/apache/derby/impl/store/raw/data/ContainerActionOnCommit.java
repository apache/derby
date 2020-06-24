/*

   Derby - Class org.apache.derby.impl.store.raw.data.ContainerActionOnCommit

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

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.iapi.services.monitor.DerbyObserver;
import org.apache.derby.iapi.store.raw.ContainerKey;

/**
	An Observer that can be attached to a transaction to
	implement some action when the transaction
	commits or rollsback in some way.
*/

abstract class ContainerActionOnCommit implements DerbyObserver {
//IC see: https://issues.apache.org/jira/browse/DERBY-6856

	protected ContainerKey identity;

	protected ContainerActionOnCommit(ContainerKey identity) {

		this.identity = identity;
	}

	public int hashCode() {
		return identity.hashCode();
	}

	/**
		An equals method that returns true if the other obejct
		is a sub-class of this, and the container identities
		are equal *and* it is the same class as this.
		<BR>
		This allows mutiple additions of value equality
		obejcts to the observer list while only retaining one.
	*/
	public boolean equals(Object other) {
		if (other instanceof ContainerActionOnCommit) {
			if (!identity.equals(((ContainerActionOnCommit) other).identity))
				return false;

			// the class of the types must match as well
			return getClass().equals(other.getClass());
		}
		return false;
	}
}
