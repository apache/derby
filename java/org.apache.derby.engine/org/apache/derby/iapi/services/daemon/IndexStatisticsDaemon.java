/*

   Derby - Class org.apache.derby.iapi.services.daemon.IndexStatisticsDaemon

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

package org.apache.derby.iapi.services.daemon;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

/**
 * <p>
 * Daemon acting as a coordinator for creating and updating index statistics.
 * </p>
 * <p>
 * There are two modes of operation:
 * </p>
 * <ul> <li>explicit - generates index statistics due to an explict request from
 *          the user. The entrypoint is <tt>runExplicitly</tt>.</li>
 *      <li>background - generates index statistics as a background task due to
 *          an event that has triggered a statistics update. The entrypoint
 *          is <tt>schedule</tt>.</li>
 * </ul>
 * <p>
 * The modes differ in how the operation affects other operations in the running
 * system, and also how errors are dealt with. The background mode will try to
 * affect other operations as little as possible, and errors won't be reported
 * unless they are severe. The explicit mode will do more to make sure the
 * operation succeeds (for instance by using locks), and will report all errors.
 * </p>
 */
public interface IndexStatisticsDaemon {

    /**
     * Creates/updates index statistics for the specified conglomerates/indexes.
     *
     * @param lcc connection used to carry out the work
     * @param td base table
     * @param cds index conglomerates (non-index conglomerates are ignored)
     * @param runContext descriptive text for the context in which the work is
     *      being run (i.e. ALTER TABLE)
     * @throws StandardException if something goes wrong
     */
    public void runExplicitly(LanguageConnectionContext lcc,
                              TableDescriptor td,
                              ConglomerateDescriptor[] cds,
                              String runContext)
            throws StandardException;

    /**
     * Schedules creation/update of the index statistics associated with the
     * specified table.
     * <p>
     * Note that the scheduling request may be denied. Typical situations where
     * that will happen is if the work queue is full, or if work has already
     * been scheduled for the specified table.
     *
     * @param td base table
     */
    public void schedule(TableDescriptor td);

    /**
     * Stops the background daemon.
     * <p>
     * Any ongoing tasks will be aborted as soon as possible, and it will not
     * be possible to schedule new tasks. Note that <tt>runExplicitly</tt> can
     * still be used.
     */
    public void stop();
}
