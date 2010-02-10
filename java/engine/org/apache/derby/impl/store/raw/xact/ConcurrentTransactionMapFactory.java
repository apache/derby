/*

   Derby - Class org.apache.derby.impl.store.raw.xact.ConcurrentTransactionMapFactory

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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.store.raw.xact.TransactionId;

/**
 * A helper class that enables use of {@code ConcurrentHashMap} instead of
 * {@code Hashtable} on platforms that support it. This class will be used if
 * we are running on a Java 1.5 or higher.
 */
class ConcurrentTransactionMapFactory extends TransactionMapFactory {
    @Override
    Map newMap() {
        Map<TransactionId, TransactionTableEntry> map =
                new ConcurrentHashMap<TransactionId, TransactionTableEntry>();

        if (SanityManager.DEBUG) {
            // Add some extra type checks to detect bugs earlier
            map = Collections.checkedMap(
                    map, TransactionId.class, TransactionTableEntry.class);
        }

        return map;
    }

    @Override
    void visitEntries(Map map, TransactionTable.EntryVisitor visitor) {
        for (Object entry : map.values()) {
            if (!visitor.visit((TransactionTableEntry) entry)) {
                // The visitor returned false, meaning that it's done with
                // all of its work and we can stop the scan.
                break;
            }
        }
    }
}
