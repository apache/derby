/*

   Derby - Class org.apache.derby.iapi.transaction.TransactionListener

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package org.apache.derby.iapi.transaction;

import org.apache.derby.shared.common.error.StandardException;

/**
 * An interface that must be implemented by a object that
 * wants to be notified when a significant transaction event occurs.
 */
public interface TransactionListener {
    
    /**
     * Notifies registered listener that the transaction
     * is about to commit. Called before the commit is
     * recorded and flushed to the transaction log device.
     * 
     * @return true to remove this listener once this
     * method returns.
     * 
     * @throws StandardException If thrown the commit attempt
     * will be stopped and instead the transaction will be rolled back.
     */
    boolean preCommit() throws StandardException;
    
    /**
     * Notifies registered listener that the transaction
     * is about to rollback. Called before any physical rollback.
     * The listener will be removed from the current transaction
     * once the method returns.
     * 
     * @throws StandardException If thrown the rollback attempt
     * will be stopped and instead the database will be shut down.
     * 
     * TODO: Define behaviour on exception during rollback.
     */
    void preRollback() throws StandardException;

    // to support statement/savepoint rollback.
    // void preSavepointRollback() throws StandardException;

}
