/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.raw.data
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.iapi.store.raw.ContainerKey;

import java.util.Observer;

/**
	An Observer that can be attached to a transaction to
	implement some action when the transaction
	commits or rollsback in some way.
*/

abstract class ContainerActionOnCommit implements Observer {

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
