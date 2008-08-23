/*

   Derby - Class org.apache.derby.iapi.services.locks.CompatibilitySpace

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

package org.apache.derby.iapi.services.locks;

/**
 * <p> This interface must be implemented by objects returned from
 * <code>LockFactory.createCompatibilitySpace()</code>. </p>
 *
 * <p> A <code>CompatibilitySpace</code> can have an owner (for instance a
 * transaction). Currently, the owner is used by the virtual lock table to find
 * out which transaction a lock belongs to. Some parts of the code also use the
 * owner as a group object which guarantees that the lock is released on a
 * commit or an abort. The owner has no special meaning to the lock manager and
 * can be any object, including <code>null</code>. </p>
 *
 * @see LockFactory#createCompatibilitySpace
 */
public interface CompatibilitySpace {
    /**
     * Gets an object representing the owner of the compatibility space.
     *
     * @return object representing the owner of the compatibility space, or
     * <code>null</code> if no owner has been specified.
     */
    LockOwner getOwner();
}
