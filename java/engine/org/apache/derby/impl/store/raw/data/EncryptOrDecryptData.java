/*

   Derby - Class org.apache.derby.impl.store.raw.data.EncryptOrDecryptData

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

import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.raw.data.RawContainerHandle;
import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.io.StorageFactory;
import org.apache.derby.io.StorageFile;
import java.security.AccessController;
import java.security.PrivilegedAction;


/**
 * This class is used to encrypt all the containers in the data segment with a
 * new encryption key when password/key is changed or when an existing database
 * is reconfigured for encryption.
 *
 * Encryption of existing data in the data segments is done by doing the
 * following:
 *  Find all the containers in data segment (seg0) and encrypt all of them
 *  with the new  encryption key, the process for each container is:
 *   1.Write a log record to indicate that the container is getting encrypted.
 *   2.Read all the pages of the container through the page cache and
 *       encrypt each page with new encryption key and then write to a
 *       temporary file(n<cid>.dat) in the data segment itself.
 *   3. Rename the current container file (c<cid>.dat) to
 *                                         another file (o<cid>.dat)
 *   4. Rename the new encrypted version of the file (n&lt;cid).dat) to be
 *                                    the current container file (c<cid>.dat).
 *   5. All the old version of  the container (o<cid>.dat) files are removed
 *      after a successful checkpoint with a new key or on a rollback.
 */

public class EncryptOrDecryptData implements PrivilegedAction<Boolean> {

    private BaseDataFileFactory dataFactory;
    private StorageFactory storageFactory;

    /* privileged actions */
    private static final int STORAGE_FILE_EXISTS_ACTION = 1;
    private static final int STORAGE_FILE_DELETE_ACTION = 2;
    private static final int STORAGE_FILE_RENAME_ACTION = 3;
    private int actionCode;
    private StorageFile actionStorageFile;
    private StorageFile actionDestStorageFile;

    public EncryptOrDecryptData(BaseDataFileFactory dataFactory) {
        this.dataFactory = dataFactory;
        this.storageFactory = dataFactory.getStorageFactory();
    }

    /**
     * Finds all the all the containers stored in the data directory and
     * decrypts them.
     *
     * @param t the transaction that is used for the decryption operation
     * @throws StandardException Standard Derby error policy
     */
    public void decryptAllContainers(RawTransaction t)
            throws StandardException {
        encryptOrDecryptAllContainers(t, false);
    }

    /**
     * Find all the all the containers stored in the data directory and
     * encrypt them.
     *
     * @param t the transaction that is used for the encryption operation
     * @exception StandardException Standard Derby error policy
     */
    public void encryptAllContainers(RawTransaction t)
            throws StandardException {
        encryptOrDecryptAllContainers(t, true);
    }

    /**
     * Encrypts or decrypts all containers in the database data directory.
     *
     * @param t transaction used for the cryptographic operation
     * @param doEncrypt tells whether to encrypt or decrypt
     * @exception StandardException Standard Derby error policy
     */
    private void encryptOrDecryptAllContainers(RawTransaction t,
                                               boolean doEncrypt)
            throws StandardException {
        // List of containers that need to be transformed are identified by
        // simply reading the list of files in seg0.
        String[] files = dataFactory.getContainerNames();
        if (files != null) {
            long segmentId = 0;

            // Loop through all the files in seg0 and
            // encrypt/decrypt all valid containers.
            for (int f = files.length-1; f >= 0 ; f--) {
                long containerId;
                try {
                    containerId =
                        Long.parseLong(files[f].substring(1,
                                       (files[f].length() -4)), 16);
                }
                catch (Throwable th)
                {
                    // ignore errors from parse, it just means
                    // that someone put a file in seg0 that we
                    // didn't expect.  Continue with the next one.
                    continue;
                }

                ContainerKey ckey = new ContainerKey(segmentId,
                                                     containerId);
                encryptOrDecryptContainer(t, ckey, doEncrypt);
            }

            // Old versions of the container files will
            // be removed after the (re)encryption of database
            // is completed.
        } else {
            if (SanityManager.DEBUG) {
                SanityManager.THROWASSERT(
                        (doEncrypt ? "encryption" : "decryption") +
                        " process is unable to read container names in seg0");
            }
        }

    }


    /**
     * Encrypts or decrypts the specified container.
     *
     * @param t transaction that used to perform the cryptographic operation
     * @param ckey the key of the container that is being encrypted/decrypted
     * @param doEncrypt tells whether to encrypt or decrypt
     * @exception StandardException Standard Derby error policy
     */
    private void encryptOrDecryptContainer(RawTransaction t,
                                           ContainerKey ckey,
                                           boolean doEncrypt)
        throws StandardException
    {

        LockingPolicy cl =
            t.newLockingPolicy(
                               LockingPolicy.MODE_CONTAINER,
                               TransactionController.ISOLATION_SERIALIZABLE,
                               true);

        if (SanityManager.DEBUG )
            SanityManager.ASSERT(cl != null);

        RawContainerHandle containerHdl = (RawContainerHandle)
            t.openContainer(ckey, cl, ContainerHandle.MODE_FORUPDATE);

        if (SanityManager.DEBUG )
            SanityManager.ASSERT(containerHdl != null);

        EncryptContainerOperation lop =
            new EncryptContainerOperation(containerHdl);
        t.logAndDo(lop);

        // flush the log to reduce the window between where
        // the encrypted container is created & synced and the
        // log record for it makes it to disk. if we fail during
        // encryption of the container, log record will make sure
        // container is restored to the original state and
        // any temporary files are cleaned up.
        dataFactory.flush(t.getLastLogInstant());

        // encrypt the container.
        String newFilePath = getFilePath(ckey, false);
        StorageFile newFile = storageFactory.newStorageFile(newFilePath);
        containerHdl.encryptOrDecryptContainer(newFilePath, doEncrypt);
        containerHdl.close();


        /*
         * Replace the current container file with the new container file after
         * keeping a copy of the current container file, it will be removed on
         * after a checkpoint with new key or on a rollback this copy will be
         * replace the container file to bring the database back to the
         * state before encryption process started.
         */

        // discard pages in the cache related to this container.
        if (!dataFactory.getPageCache().discard(ckey)) {
            if (SanityManager.DEBUG )
                SanityManager.THROWASSERT("unable to discard pages releated to " +
                                          "container " + ckey  +
                                          " from the page cache");
        }


        // get rid of the container entry from conatainer cache
        if (!dataFactory.getContainerCache().discard(ckey)) {
            if (SanityManager.DEBUG )
                SanityManager.THROWASSERT("unable to discard a container " +
                                          ckey + " from the container cache");
        }

        StorageFile currentFile =  dataFactory.getContainerPath(ckey , false);
        StorageFile oldFile = getFile(ckey, true);

        if (!privRename(currentFile, oldFile)) {
                throw StandardException.
                    newException(SQLState.RAWSTORE_ERROR_RENAMING_FILE,
                                 currentFile, oldFile);
            }

        // now replace current container file with the new file.
        if (!privRename(newFile, currentFile)) {
            throw StandardException.
                newException(SQLState.RAWSTORE_ERROR_RENAMING_FILE,
                             newFile, currentFile);

        }
    }


    /**
     * Get file handle to a container file that is used to keep
     * temporary versions of the container file.
     */
    private StorageFile getFile(ContainerKey containerId, boolean old) {
        return storageFactory.newStorageFile(getFilePath(containerId,
                                                         old));
    }

    /**
     * Get path to a container file that is used to keep temporary versions of
     * the container file.
     */
    private String getFilePath(ContainerKey containerId, boolean old) {
        StringBuffer sb = new StringBuffer("seg");
        sb.append(containerId.getSegmentId());
        sb.append(storageFactory.getSeparator());
        sb.append(old ? 'o' : 'n');
        sb.append(Long.toHexString(containerId.getContainerId()));
        sb.append(".dat");
        return sb.toString();
    }

    private boolean isOldContainerFile(String fileName) {
        // Old versions start with prefix "o" and ends with ".dat".
        return (fileName.startsWith("o") && fileName.endsWith(".dat"));
    }

    private StorageFile getFile(String ctrFileName)
    {
        long segmentId = 0;
        StringBuffer sb = new StringBuffer("seg");
        sb.append(segmentId);
        sb.append(storageFactory.getSeparator());
        sb.append(ctrFileName);
        return storageFactory.newStorageFile(sb.toString());
    }

    /* Restore the contaier to the state it was before
     * it was encrypted with new encryption key. This function is
     * called during undo of the EncryptContainerOperation log record
     * incase of a error/crash before database was successfuly configured with
     * new encryption properties.
     * @param ckey the key of the container that needs to be restored.
     * @exception StandardException Standard Derby error policy
     */
    void restoreContainer(ContainerKey containerId)
        throws StandardException
    {

        // get rid of the container entry from conatainer cache,
        // this will make sure there are no file opens on the current
        // container file.

        if (!dataFactory.getContainerCache().discard(containerId)) {
            if (SanityManager.DEBUG )
                SanityManager.THROWASSERT(
                  "unable to discard  container from cache:" +
                  containerId);
        }

        StorageFile currentFile = dataFactory.getContainerPath(containerId,
                                                               false);
        StorageFile oldFile = getFile(containerId, true);
        StorageFile newFile = getFile(containerId, false);

        // if backup of the original container file exists, replace the
        // container with the backup copy.
        if (privExists(oldFile)) {
            if (privExists(currentFile)) {
                // rename the current container file to be the new file.
                if (!privRename(currentFile, newFile)) {
                    throw StandardException.
                        newException(SQLState.RAWSTORE_ERROR_RENAMING_FILE,
                                     currentFile, newFile);
                }
            }

            if (!privRename(oldFile, currentFile)) {
                throw StandardException.
                    newException(SQLState.RAWSTORE_ERROR_RENAMING_FILE,
                                 oldFile, currentFile);
            }
        }

        // if the new copy of the container file exists, remove it.
        if (privExists(newFile)) {

            if (!privDelete(newFile))
                throw StandardException.newException(
                                                 SQLState.UNABLE_TO_DELETE_FILE,
                                                 newFile);
        }
    }


    /**
     * Removes old versions of the containers after a cryptographic operation
     * on the database.
     */
    public void removeOldVersionOfContainers()
            throws StandardException {
        // Find the old version of the container files and delete them.
        String[] files = dataFactory.getContainerNames();
        if (files != null) {
            // Loop through all the files in seg0 and
            // delete all old copies of the containers.
            for (int i = files.length-1; i >= 0 ; i--) {
                if (isOldContainerFile(files[i])) {
                    StorageFile oldFile = getFile(files[i]);
                    if (!privDelete(oldFile)) {
                        throw StandardException.newException(
                                  SQLState.FILE_CANNOT_REMOVE_ENCRYPT_FILE,
                                  oldFile);
                    }
                }
            }
        }
    }

    private synchronized boolean privExists(StorageFile file)
    {
        actionCode = STORAGE_FILE_EXISTS_ACTION;
        actionStorageFile = file;
        Boolean ret = AccessController.doPrivileged(this);
        actionStorageFile = null;
        return ret.booleanValue();

    }


    private synchronized boolean privDelete(StorageFile file)
    {
        actionCode = STORAGE_FILE_DELETE_ACTION;
        actionStorageFile = file;
        Boolean ret = AccessController.doPrivileged(this);
        actionStorageFile = null;
        return ret.booleanValue();

    }

    private synchronized boolean privRename(StorageFile fromFile,
                                            StorageFile destFile)
    {
        actionCode = STORAGE_FILE_RENAME_ACTION;
        actionStorageFile = fromFile;
        actionDestStorageFile = destFile;
        Boolean ret = AccessController.doPrivileged(this);
        actionStorageFile = null;
        actionDestStorageFile = null;
        return ret.booleanValue();

    }



    // PrivilegedAction method
    public Boolean run()
    {
        switch(actionCode)
        {
        case STORAGE_FILE_EXISTS_ACTION:
            return actionStorageFile.exists();
        case STORAGE_FILE_DELETE_ACTION:
            return actionStorageFile.delete();
        case STORAGE_FILE_RENAME_ACTION:
            return actionStorageFile.renameTo(actionDestStorageFile);
        }

        return null;
    }
}
