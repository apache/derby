/*

   Derby - Class org.apache.impl.storeless.NoOpTransaction

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
package org.apache.derby.impl.storeless;

import java.io.Serializable;
import java.util.Properties;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.iapi.services.locks.CompatibilitySpace;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.BackingStoreHashtable;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DatabaseInstant;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.FileResource;
import org.apache.derby.iapi.store.access.GroupFetchScanController;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowLocationRetRowSource;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.SortController;
import org.apache.derby.iapi.store.access.SortCostController;
import org.apache.derby.iapi.store.access.SortObserver;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.StoreCostController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;

/**
 * A TransactionController that does nothing.
 * This allows the existing transaction aware language
 * code to remain unchanged while not supporting transactions
 * for the storeless engine.
 */
class NoOpTransaction implements TransactionController {

    public AccessFactory getAccessManager() {
        // Auto-generated method stub
        return null;
    }

    public boolean conglomerateExists(long conglomId) throws StandardException {
        // Auto-generated method stub
        return false;
    }

    public long createConglomerate(String implementation,
            DataValueDescriptor[] template, ColumnOrdering[] columnOrder,
            int[] collation_ids,
            Properties properties, int temporaryFlag) throws StandardException {
        // Auto-generated method stub
        return 0;
    }

    public long createAndLoadConglomerate(String implementation,
            DataValueDescriptor[] template, ColumnOrdering[] columnOrder,
            int[] collation_ids,
            Properties properties, int temporaryFlag,
            RowLocationRetRowSource rowSource, long[] rowCount)
            throws StandardException {
        // Auto-generated method stub
        return 0;
    }

    public long recreateAndLoadConglomerate(String implementation,
            boolean recreate_ifempty, DataValueDescriptor[] template,
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
            ColumnOrdering[] columnOrder, 
            int[] collation_ids,
            Properties properties,
            int temporaryFlag, long orig_conglomId,
            RowLocationRetRowSource rowSource, long[] rowCount)
            throws StandardException {
        // Auto-generated method stub
        return 0;
    }

    public void addColumnToConglomerate(long conglomId, int column_id,
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
            Storable template_column, int collation_id) throws StandardException {
        // Auto-generated method stub

    }

    public void dropConglomerate(long conglomId) throws StandardException {
        // Auto-generated method stub

    }

    public long findConglomid(long containerid) throws StandardException {
        // Auto-generated method stub
        return 0;
    }

    public long findContainerid(long conglomid) throws StandardException {
        // Auto-generated method stub
        return 0;
    }

    public TransactionController startNestedUserTransaction(
    boolean readOnly,
    boolean flush_log_on_xact_end)
            throws StandardException {
        return this;
    }

    public Properties getUserCreateConglomPropList() {
        // Auto-generated method stub
        return null;
    }

    public ConglomerateController openConglomerate(long conglomId,
            boolean hold, int open_mode, int lock_level, int isolation_level)
            throws StandardException {
        // Auto-generated method stub
        return null;
    }

    public ConglomerateController openCompiledConglomerate(boolean hold,
            int open_mode, int lock_level, int isolation_level,
            StaticCompiledOpenConglomInfo static_info,
            DynamicCompiledOpenConglomInfo dynamic_info)
            throws StandardException {
        // Auto-generated method stub
        return null;
    }

    public BackingStoreHashtable createBackingStoreHashtableFromScan(
            long conglomId, int open_mode, int lock_level, int isolation_level,
            FormatableBitSet scanColumnList,
            DataValueDescriptor[] startKeyValue, int startSearchOperator,
            Qualifier[][] qualifier, DataValueDescriptor[] stopKeyValue,
            int stopSearchOperator, long max_rowcnt, int[] key_column_numbers,
            boolean remove_duplicates, long estimated_rowcnt,
            long max_inmemory_rowcnt, int initialCapacity, float loadFactor,
            boolean collect_runtimestats, boolean skipNullKeyColumns,
            boolean keepAfterCommit,
            boolean includeRowLocations)
            throws StandardException {
        // Auto-generated method stub
        return null;
    }

    public ScanController openScan(long conglomId, boolean hold, int open_mode,
            int lock_level, int isolation_level,
            FormatableBitSet scanColumnList,
            DataValueDescriptor[] startKeyValue, int startSearchOperator,
            Qualifier[][] qualifier, DataValueDescriptor[] stopKeyValue,
            int stopSearchOperator) throws StandardException {
        // Auto-generated method stub
        return null;
    }

    public ScanController openCompiledScan(boolean hold, int open_mode,
            int lock_level, int isolation_level,
            FormatableBitSet scanColumnList,
            DataValueDescriptor[] startKeyValue, int startSearchOperator,
            Qualifier[][] qualifier, DataValueDescriptor[] stopKeyValue,
            int stopSearchOperator, StaticCompiledOpenConglomInfo static_info,
            DynamicCompiledOpenConglomInfo dynamic_info)
            throws StandardException {
        // Auto-generated method stub
        return null;
    }

    public GroupFetchScanController openGroupFetchScan(long conglomId,
            boolean hold, int open_mode, int lock_level, int isolation_level,
            FormatableBitSet scanColumnList,
            DataValueDescriptor[] startKeyValue, int startSearchOperator,
            Qualifier[][] qualifier, DataValueDescriptor[] stopKeyValue,
            int stopSearchOperator) throws StandardException {
        // Auto-generated method stub
        return null;
    }

    public GroupFetchScanController defragmentConglomerate(long conglomId,
            boolean online, boolean hold, int open_mode, int lock_level,
            int isolation_level) throws StandardException {
        // Auto-generated method stub
        return null;
    }

    public void purgeConglomerate(long conglomId) throws StandardException {
        // Auto-generated method stub

    }

    public void compressConglomerate(long conglomId) throws StandardException {
        // Auto-generated method stub

    }

    public boolean fetchMaxOnBtree(long conglomId, int open_mode,
            int lock_level, int isolation_level,
            FormatableBitSet scanColumnList, DataValueDescriptor[] fetchRow)
            throws StandardException {
        // Auto-generated method stub
        return false;
    }

    public StoreCostController openStoreCost(long conglomId)
            throws StandardException {
        // Auto-generated method stub
        return null;
    }

    public int countOpens(int which_to_count) throws StandardException {
        // Auto-generated method stub
        return 0;
    }

    public String debugOpened() throws StandardException {
        // Auto-generated method stub
        return null;
    }

    public FileResource getFileHandler() {
        // Auto-generated method stub
        return null;
    }

    public CompatibilitySpace getLockSpace() {
        // Auto-generated method stub
        return null;
    }

    public StaticCompiledOpenConglomInfo getStaticCompiledConglomInfo(
            long conglomId) throws StandardException {
        // Auto-generated method stub
        return null;
    }

    public DynamicCompiledOpenConglomInfo getDynamicCompiledConglomInfo(
            long conglomId) throws StandardException {
        // Auto-generated method stub
        return null;
    }

    public long[] getCacheStats(String cacheName) {
        // Auto-generated method stub
        return null;
    }

    public void resetCacheStats(String cacheName) {
        // Auto-generated method stub

    }

    public void logAndDo(Loggable operation) throws StandardException {
        // Auto-generated method stub

    }

    public long createSort(Properties implParameters,
            DataValueDescriptor[] template, ColumnOrdering[] columnOrdering,
            SortObserver sortObserver, boolean alreadyInOrder,
            long estimatedRows, int estimatedRowSize) throws StandardException {
        // Auto-generated method stub
        return 0;
    }

    public void dropSort(long sortid) throws StandardException {
        // Auto-generated method stub

    }

    public SortController openSort(long id) throws StandardException {
        // Auto-generated method stub
        return null;
    }

    public SortCostController openSortCostController()
            throws StandardException {
        // Auto-generated method stub
        return null;
    }

    public RowLocationRetRowSource openSortRowSource(long id)
            throws StandardException {
        // Auto-generated method stub
        return null;
    }

    public ScanController openSortScan(long id, boolean hold)
            throws StandardException {
        // Auto-generated method stub
        return null;
    }

    public boolean anyoneBlocked() {
        // Auto-generated method stub
        return false;
    }

    public void abort() throws StandardException {
        // Auto-generated method stub

    }

    public void commit() throws StandardException {
        // Auto-generated method stub

    }

    public DatabaseInstant commitNoSync(int commitflag)
            throws StandardException {
        // Auto-generated method stub
        return null;
    }

    public void destroy() {
        // Auto-generated method stub

    }

    public ContextManager getContextManager() {
        // Auto-generated method stub
        return null;
    }

    public String getTransactionIdString() {
        // Auto-generated method stub
        return null;
    }

    public String getActiveStateTxIdString() {
        // Auto-generated method stub
        return null;
    }

    public boolean isIdle() {
        // Auto-generated method stub
        return false;
    }

    public boolean isGlobal() {
        // Auto-generated method stub
        return false;
    }

    public boolean isPristine() {
        // Auto-generated method stub
        return false;
    }

    public int releaseSavePoint(String name, Object kindOfSavepoint)
            throws StandardException {
        // Auto-generated method stub
        return 0;
    }

    public int rollbackToSavePoint(String name, boolean close_controllers,
            Object kindOfSavepoint) throws StandardException {
        // Auto-generated method stub
        return 0;
    }

    public int setSavePoint(String name, Object kindOfSavepoint)
            throws StandardException {
        // Auto-generated method stub
        return 0;
    }

    public Object createXATransactionFromLocalTransaction(int format_id,
            byte[] global_id, byte[] branch_id) throws StandardException {
        // Auto-generated method stub
        return null;
    }

    public Serializable getProperty(String key) throws StandardException {
        // Auto-generated method stub
        return null;
    }

    public Serializable getPropertyDefault(String key) throws StandardException {
        // Auto-generated method stub
        return null;
    }

    public boolean propertyDefaultIsVisible(String key)
            throws StandardException {
        // Auto-generated method stub
        return false;
    }

    public void setProperty(String key, Serializable value,
            boolean dbOnlyProperty) throws StandardException {
        // Auto-generated method stub

    }

    public void setPropertyDefault(String key, Serializable value)
            throws StandardException {
        // Auto-generated method stub

    }

    public Properties getProperties() throws StandardException {
        // Auto-generated method stub
        return null;
    }
    public DataValueFactory getDataValueFactory() throws StandardException {
        // Auto-generated method stub
        return(null);
    }

    public void setNoLockWait(boolean noWait) {
        // Auto-generated method stub
    }

}
