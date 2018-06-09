/*

   Derby - Class org.apache.derby.impl.store.raw.data.PageCreationArgs

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

package org.apache.derby.impl.store.raw.data;

/**
 * This class holds information that is passed to {@code
 * CachedPage.createPage()} and used when a page object (either a
 * {@code StoredPage} or an {@code AllocPage}) is created.
 */
class PageCreationArgs {
    /**
     * Tells which type of page to create. Either
     * {@code StoredPage.FORMAT_NUMBER} or {@code AllocPage.FORMAT_NUMBER}.
     */
    final int formatId;

    /**
     * Tells whether writes to this page should be synced. Should be
     * {@code CachedPage.WRITE_SYNC} or {@code CachedPage.WRITE_NO_SYNC}, or
     * 0 if the page is in a temporary container.
     */
    final int syncFlag;

    /** The size of the page in bytes. */
    final int pageSize;

    /** % of page to keep free for updates. Not used for {@code AllocPage}. */
    final int spareSpace;

    /** Minimum space to reserve for record portion length of row. */
    final int minimumRecordSize;

    /** Size of the container information stored in the {@code AllocPage}. */
    final int containerInfoSize;

    PageCreationArgs(int formatId, int syncFlag, int pageSize, int spareSpace,
                     int minimumRecordSize, int containerInfoSize) {
        this.formatId = formatId;
        this.syncFlag = syncFlag;
        this.pageSize = pageSize;
        this.spareSpace = spareSpace;
        this.minimumRecordSize = minimumRecordSize;
        this.containerInfoSize = containerInfoSize;
    }
}
