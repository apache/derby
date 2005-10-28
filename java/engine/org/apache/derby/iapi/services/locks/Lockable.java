/*

   Derby - Class org.apache.derby.iapi.services.locks.Lockable

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

package org.apache.derby.iapi.services.locks;

import java.util.Hashtable;

/**
	Any object that needs to be locked must implement Lockable.
	This allows a generic lock manager that can have locking policies
	defined on a per-object basis.

    A request to lock the object takes a qualifier, this qualifier may be
	used the object to implement a complex locking policy, e.g. traditional
	database shared, update and exclusive locks. 
	<P>
	The lock manager uses this ordered protocol to determine if a lock request on a
	Lockable <TT> L </TT> with qualifier <TT> Q1 </TT> in compatiblity space
	<TT> CS1 </TT> can be granted:
	<OL>
	<LI>If no locks are held on <TT> L </TT> in any compatability space then the
	request is granted.
	<LI>If <TT>L.requestCompatible(Q1)</TT> returns true then the lock is granted.
	<LI>Otherwise the request is granted if the following expression evaluates
	to true for every other lock <TT>{ CSn, Qn}</TT> held on <TT> L </TT>
	<UL>
	<LI> <PRE>    ( ( CSn == CS1 ) && L.lockerAlwaysCompatible() ) </PRE>
	<LI> <PRE> || (L.reqestCompatible(Q1, Qn)) </PRE>
	</UL>
	</OL>
	<BR>
	If the request is granted then a call is made to <TT> L.lockEvent(CS1, Q1) </TT>.
	<BR>
	When the lock is released a call is made to <TT> L.unlockEvent(CS1, Q1) </TT>.
    <P>
	The lock manager uses equals() and hashCode() to identify unique Lockables.
	<BR>
	If the class implementing Lockable requires that each instance of class
	correspond to a different locked object then the equals() method must test
	equality via the reference equality test (==), this is the default behaviour
	for equality.
	<BR>
	If the class implementing Lockable require that each instance of the class
	that has the same value (as defined by the class) corresponds to a locked
	object then its equals() method must reflect that, e.g. by testing equality
	of its fields. In this case the first Lockable to be locked will be kept
	by lock manager as the key for the lock. Thus even after the first caller
	unlocks the obejct, its reference will still be kept by the lock manager.
	Thus Lockable's that per value equality must be designed so that they
	are never re-used for different lockable concepts.
	<BR>
	In either case the equals() method must accept a reference to an object of
	a different type.
	<BR>
	As per standard hashtable rules the value returned by hashCode() must be in sync
	with the equals() method.

	<BR>
	MT - Mutable - : single thread required, synchronization is provided by the lock manager.
	If the class implementing Lockable uses value equality then it must have an immutable identity.
*/

public interface Lockable {
    
	/**
		Note the fact the object is locked. Performs required actions
		to ensure that unlockEvent() work correctly.
		This method does not actually  perform any locking of the
		object, the locking mechanism is provided by the lock manager.
		<P>
		If the class supports multiple lockers of the object then this method
		will be called once per locker, each with their own qualifier.
		<P>
		Must only be called by the lock manager. Synchronization will be handled
		by the lock manager.
	*/
	public void lockEvent(Latch lockInfo);

	/**
		Return true if the requested qualifier is compatible with the already granted
		qualifier.
	*/
	public boolean requestCompatible(Object requestedQualifier, Object grantedQualifier);

	/**
		Returns true if any lock request on a Lockable L in a compatibility space CS1 is compatible
		with any other lock held on L in CS1.

	*/
	public boolean lockerAlwaysCompatible();

	/**
		Note that the object has been unlocked 
		<P>
		Must only be called by the lock manager. Synchronization will be handled
		by the lock manager.
	*/
	public void unlockEvent(Latch lockInfo);

	/**
		If this lockable object wants to participate in a diagnostic virtual
		lock table, then put any relavent attributes of this lock into the
		attributes list (the attribute must be an immutable object).  The list
		of attributes of interest to the virtual lock table can be found in
		VirtualLockTable. The toString method will be called by the VirtualTable
		on the attribute value for display. 
		<P>
		@param flag use the bits in this int to decide if the user is
		interested in this kind of lockable object.  The bits are defined in
		VirtualLockTable.  For instance, the user may only ask
		for TABLE_AND_ROWLOCK and if this is not a table or row lock, then
		don't paritipate. 
		@param attributes if this decides to participate, put all relavent
		attributes into the Hashtable.  The complete list of interesting
		attributes is listed in VirtualLockTable.
		The following attributes must be present for all participating
		lockables:
		VirtualLockTable.LOCKNAME,
		VirtualLockTable.LOCKTYPE,
		either VirtualLockTable.CONTAINERID or VirtualLockTable.CONGLOMID,
		<P>
		MT - this routine must be MP safe, caller will not be single threading
		the lock manager.
		<P>
		@return true if this object has diagnostic information to add to the
		virtual lock table.  If this object either does not want to participate
		in the diagnostic virtual lock table or none of the attributes
		requested are attributes of this lock, returns false.

		@see VirtualLockTable
	 */
	public boolean lockAttributes(int flag, Hashtable attributes);
}
