/*

   Derby - Class org.apache.derby.iapi.store.access.DatabaseInstant

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

import java.io.Serializable;
/**
 *
 *	
 * A DatabaseInstant is a quantity which the database associates
 *  with events to collate them.
 * 
 * This interface is used in the column SYS.SYSSYNCINSTANTS.INSTANT.
 * <P>
 * Assume a database associates a DatabaseInstant to an event E1. We call this
 * I(E1). Also assume the same Database associates a DatabaseInstant to a second
 * event E2. We call this I(E2). By definition
 *
 * <OL>
 * <LI> If I(E1) < I(E2) event E1 occurred before event E2
 * <LI> If I(E2) = I(E2) event E1 is the same event as E2
 * <LI> If I(E1) > I(E2) event E1 occurred after event E2
 * </OL>
 *
 * <P>It is not meaningful to compare a DatabaseInstant from one database with a
 * DatabaseInstant from another. The result of such a comparison is
 * undefined. Because a database may construct, store and compare huge numbers
 * of DatabaseInstants, this interface does not require an implementation to
 * notice when a caller compares a DatabaseInstants from different databases.
 * <P>
 * Any implementation of this interface must implement value equality, thus
 * implementing equals() and hashCode() methods.
 */
public interface DatabaseInstant
extends Serializable
{

	/**
	  Return true if this DatabaseInstant is before another
	  DatabaseInstant from the same database.

	  @param other a DatabaseInstant from the same database as
	  this. 
	  
	  @return the comparison result. If 'other' is from another database
	  the result is undefined.  
	*/
	public boolean lessThan(DatabaseInstant other);

	/**
	  Return true if this DatabaseInstant equals
	  DatabaseInstant from the same database.

	  @param other a DatabaseInstant from the same database as
	  this. 
	  
	  @return the comparison result. If 'other' is from another database
	  the result is undefined.  
	*/
	public boolean equals(Object other);

    /**
     * Return the next higher DatabaseInstant. There is no requirement that
     * a transaction with the next instant exist in the database. It is required that
     * this.lessThan( this.next()), and that no instant can be between this and this.next().
     *
     * If the DatabaseInstant is implemented using a integer then next() should return
     * a new DatabaseInstant formed by adding one to the integer.
     *
     * @return the next possible DatabaseInstant
     */
    public DatabaseInstant next();

    /**
     * Return the next lower DatabaseInstant. There is no requirement that
     * a transaction with the next instant exist in the database. It is required that
     * this.prior().lessThan( this), and that no instant can be between this and this.prior().
     *
     * If the DatabaseInstant is implemented using a integer then prior() should return
     * a new DatabaseInstant formed by subtracting one from the integer.
     *
     * @return the prior possible DatabaseInstant
     */
    public DatabaseInstant prior();

    /**
     * Convert the database instant to a string. This is mainly used for debugging.
     *
     * @return a string representation of the instant.
     */
    public String toString();
}
