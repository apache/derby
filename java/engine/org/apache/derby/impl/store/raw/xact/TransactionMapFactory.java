/*

   Derby - Class org.apache.derby.impl.store.raw.xact.TransactionMapFactory

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

package org.apache.derby.impl.store.raw.xact;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

/**
 * Helper class for {@code TransactionTable} which allows it to plug in a
 * different {@code java.util.Map} implementation for the map that contains
 * all the {@code TransactionTableEntry} instances in the transaction table.
 * The default implementation of this class provides support for
 * {@code java.util.Hashtable}.
 */
class TransactionMapFactory {

    /**
     * <p>
     * Create a new map instance. The map implementation must be thread-safe
     * so that its instances can be accessed concurrently by multiple threads.
     * That is, the map implemenation must guarantee that every method is
     * atomic and does not fail or make the map become inconsistent just
     * because it is accessed concurrently by another thread.
     * </p>
     *
     * <p>
     * Callers should never synchronize on the returned object in order to
     * get consistency/atomicity guarantees across multiple accesses to the
     * map. Instead, they should use the helper methods defined in this class,
     * so that a mechanism appropriate for this particular map implementation
     * is used to ensure thread-safety.
     * </p>
     *
     * @return
     */
    Map newMap() {
        return new Hashtable();
    }

    /**
     * <p>
     * Traverse the values of a map, and apply the specified visitor on each
     * value. The traversal should be thread-safe in the sense that it should
     * not fail because other threads access and potentially modify the map
     * while the traversal takes place, and it must also provide all the
     * thread-safety guarantees that are given by
     * {@link TransactionTable#visitEntries(TransactionTable.EntryVisitor)}.
     * </p>
     *
     * @param map the map that contains the
     * @param visitor
     * @see TransactionTable#visitEntries(TransactionTable.EntryVisitor)
     */
    void visitEntries(Map map, TransactionTable.EntryVisitor visitor) {
        // Since this implementation uses a Hashtable, we need to synchronize
        // on the map while iterating over it.
        synchronized (map) {
            for (Iterator it = map.values().iterator(); it.hasNext(); ) {
                if (!visitor.visit((TransactionTableEntry) it.next())) {
                    // The visitor returned false, meaning that it's done with
                    // all of its work and we can stop the scan.
                    break;
                }
            }
        }
    }
}
