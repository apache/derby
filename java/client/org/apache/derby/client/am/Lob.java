/*

   Derby - Class org.apache.derby.client.am.Lob

   Copyright (c) 2001, 2005 The Apache Software Foundation or its licensors, where applicable.

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

package org.apache.derby.client.am;

public abstract class Lob implements UnitOfWorkListener {
    // The following flags specify the data type(s) a LOB instance currently contains
    public static final int STRING = 2;
    public static final int ASCII_STREAM = 4;
    public static final int UNICODE_STREAM = 8;
    public static final int CHARACTER_STREAM = 16;
    public static final int BINARY_STREAM = 32;
    public static final int BINARY_STRING = 64;

    //---------------------navigational members-----------------------------------
    protected Agent agent_;

    //-----------------------------state------------------------------------------
    protected int dataType_ = 0;      // data type(s) the LOB instance currently contains

    protected long sqlLength_;      // length of the LOB value, as defined by the server
    protected boolean lengthObtained_;

    //---------------------constructors/finalizer---------------------------------
    protected Lob(Agent agent) {
        agent_ = agent;
        lengthObtained_ = false;
    }

    protected void finalize() throws java.lang.Throwable {
        super.finalize();
    }

    // ---------------------------jdbc 2------------------------------------------

    // should only be called by a synchronized method.


    // should only be called by a synchronized method.
    public long sqlLength() throws SqlException {
        checkForClosedConnection();

        return sqlLength_;
    }


    //-----------------------event callback methods-------------------------------

    public void listenToUnitOfWork() {
        agent_.connection_.CommitAndRollbackListeners_.add(this);
    }

    public void completeLocalCommit(java.util.Iterator listenerIterator) {
        listenerIterator.remove();
    }

    public void completeLocalRollback(java.util.Iterator listenerIterator) {
        listenerIterator.remove();
    }

    //----------------------------helper methods----------------------------------

    public Agent getAgent() {
        return agent_;
    }

    void checkForClosedConnection() throws SqlException {
        if (agent_.connection_.isClosedX()) {
            agent_.checkForDeferredExceptions();
            throw new SqlException(agent_.logWriter_, "Lob method called after connection was closed");
        } else {
            agent_.checkForDeferredExceptions();
        }
    }

    void completeLocalRollback() {
        ;
    }

    void completeLocalCommit() {
        ;
    }
}
