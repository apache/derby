/*

   Derby - Class org.apache.derby.iapi.transaction.TransactionControl

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

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.derby.iapi.error.ExceptionSeverity;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;

/**
 * Provide support to transactions to manage sets of
 * actions to perform at transaction boundaries.
 *
 * <P> Add rollback of savepoints?
 * TODO: A
 */
public final class TransactionControl {
    
    private final ArrayList listeners;
    
    public TransactionControl()
    {
        listeners = new ArrayList();
    }
    
    /**
     * Add a listener to the curent transaction.
     * 
     * A listener may be added multiple times and it will
     * receive multiple callbacks.
     * 
     */
    public void addListener(TransactionListener listener)
    {
        listeners.add(listener);
    }
    
    /**
     * Remove a listener from the current transaction.
     * 
     */
    public void removeListener(TransactionListener listener)
    {
        listeners.remove(listener);
    }
    
    /**
     * Notify all listeners that a commit is about to occur.
     * If a listener throws an exception then no
     * further listeners will be notified and a
     * StandardException with rollback severity will be thrown.
     * @throws StandardException
     */
    public void preCommitNotify() throws StandardException
    {
        if (listeners.isEmpty())
            return;
        
        for (Iterator i = listeners.iterator(); i.hasNext(); )
        {
            TransactionListener listener =
                (TransactionListener) i.next();
            
            try {
                if(listener.preCommit())
                   i.remove();
            }
            catch (StandardException se) 
            {               
                // This catches any exceptions that have Transaction severity
                // or less (e.g. Statement exception).
                // If we received any lesser
                // error then we abort the transaction anyway.
                
                if (se.getSeverity() < ExceptionSeverity.TRANSACTION_SEVERITY)
                {
                    throw StandardException.newException(
                            SQLState.XACT_COMMIT_EXCEPTION, se);
                }
                
                throw se;
                
            }
            
        }
    }
    
    /**
     * Notify all listeners that a rollback is about to occur.
     * If a listener throws an exception then no
     * further listeners will be notified and a
     * StandardException with shutdown database(?) severity will be thrown.
     * @throws StandardException
     */
    public void preRollbackNotify() throws StandardException
    { 
        if (listeners.isEmpty())
            return;
        
        for (Iterator i = listeners.iterator(); i.hasNext(); )
        {
            TransactionListener listener =
                (TransactionListener) i.next();
            
            try {
                listener.preRollback();
                i.remove();
            } catch (StandardException se) {
                // TODO: Define behaviour on exception during rollback.

                if (se.getSeverity() < ExceptionSeverity.TRANSACTION_SEVERITY)
                {
                    
                }
                throw se;
            }
        }
    }
}
