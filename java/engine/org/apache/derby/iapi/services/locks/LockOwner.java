/*
 * Derby - Class org.apache.derby.iapi.services.locks.LockOwner
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.derby.iapi.services.locks;

/**
 * Interface for classes that represent an owner of the locks within a
 * compatibility space.
 */
public interface LockOwner {
    /**
     * Tells whether lock requests should time out immediately if the lock
     * cannot be granted at once, even if {@code C_LockFactory.TIMED_WAIT}
     * was specified in the lock request.
     *
     * <p>
     *
     * Normally, this method should return {@code false}, but in some very
     * special cases it could be appropriate to return {@code true}. One
     * example is when a stored prepared statement (SPS) is compiled and stored
     * in a system table. In order to prevent exclusive locks in the system
     * table from being held until the transaction that triggered the
     * compilation is finished, the SPS will be compiled in a nested
     * transaction that is committed and releases all locks upon completion.
     * There is however a risk that the transaction that triggered the
     * compilation is holding locks that the nested transaction needs, in
     * which case the nested transaction will time out. The timeout will be
     * detected by the calling code, and the operation will be retried in the
     * parent transaction. To avoid long waits in the cases where the nested
     * transaction runs into a lock conflict with its parent, the nested
     * transaction's {@code LockOwner} instance could return {@code true} and
     * thereby making it possible to detect lock conflicts instantly.
     *
     * @return {@code true} if timed waits should time out immediately,
     * {@code false} otherwise
     */
    boolean noWait();
}
