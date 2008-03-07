/*
 
   Derby - Class org.apache.derby.impl.store.replication.master.LogShipper
 
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

package org.apache.derby.impl.store.replication.master;

import java.io.IOException;

import org.apache.derby.iapi.error.StandardException;

/**
 *
 * This is the interface for the replication log shipper. The log shipper
 * is started by the master controller service. The log shipper is responsible
 * for shipping of the log chunks from the log buffer (on the master) to the
 * slave. The log shipper handles both periodic shipping of log records as well
 * as request based shipping. The request based shipping would be useful when
 * the log buffer becomes full and needs to be freed before it can store
 * subsequent log chunks.
 *
 */
interface LogShipper {
    /**
     * updates the information about the latest instance of the log record
     * that has been flushed to the disk.
     *
     * @param latestInstanceFlushedToDisk a long that contains the latest
     *        instance of the log record that has been flushed to the disk.
     */
    public void flushedInstance(long latestInstanceFlushedToDisk);
    
    /**
     * Ships the next log record chunk, if available, from the log buffer to
     * the slave.
     *
     * @throws IOException If an exception occurs while trying to ship the
     *                     replication message (containing the log records)
     *                     across the network.
     * @throws StandardException If an exception occurs while trying to read
     *                           log records from the log buffer.
     */
    public void forceFlush() throws IOException, StandardException;
    
    /**
     *
     * Transmits all the log records in the log buffer to the slave.
     *
     * @throws IOException If an exception occurs while trying to ship the
     *                     replication message (containing the log records)
     *                     across the network.
     * @throws StandardException If an exception occurs while trying to read
     *                           log records from the log buffer.
     *
     */
    public void flushBuffer() throws IOException, StandardException;
    
    /**
     * Used to notify the log shipper that a log buffer element is full.
     */
    public void workToDo();
}
