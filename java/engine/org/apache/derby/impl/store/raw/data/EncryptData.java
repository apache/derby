/*

   Derby - Class org.apache.derby.impl.store.raw.data.EncryptData

   Copyright 1999, 2006 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.store.raw.data;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.raw.data.RawContainerHandle;
import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.io.StorageFactory;
import org.apache.derby.io.StorageFile;

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
 *   3.	Rename the current container file (c<cid>.dat) to 
 *                                         another file (o<cid>.dat)
 *   4.	Rename the new encrypted version of the file (n<cid).dat) to be 
 *                                    the current container file (c<cid>.dat).
 *   5.	Submit a post commit work to remove the old version of 
 *                                      the container (o<cid>.dat) file. 
 *   
 * 	@author  Suresh Thalamati
 */

public class EncryptData {

    private BaseDataFileFactory dataFactory;
    private StorageFactory storageFactory;

	public EncryptData(BaseDataFileFactory dataFactory) {
		this.dataFactory = dataFactory;
        this.storageFactory = dataFactory.getStorageFactory();
	}


    /*
     * Find all the all the containers stored in the data directory and 
     * encrypt them.
     * @param t the transaction that is used to configure the database 
     *          with new encryption properties.
     * @exception StandardException Standard Derby error policy
	 */
	public void encryptAllContainers(RawTransaction t) 
        throws StandardException {

        /*
		 * List of containers that needs to be encrypted are identified by 
		 * simply reading the list of files in seg0. 
		 */

		String[] files = dataFactory.getContainerNames();
		if (files != null) {
            StorageFile[] oldFiles = new StorageFile[files.length];
            int count = 0;
			long segmentId = 0;

            // loop through all the files in seg0 and 
            // encrypt all valid containers.
			for (int f = files.length-1; f >= 0 ; f--) {
				long containerId;
				try	{
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
                oldFiles[count++] = encryptContainer(t, ckey);
			}

            // remove all the old versions of the 
            // container files on post-commit.
            Serviceable removeOldFiles = new RemoveFiles(oldFiles, count);
            t.addPostCommitWork(removeOldFiles);
            
		} else
		{
			if (SanityManager.DEBUG) 
				SanityManager.THROWASSERT("encryption process is unable to" +
                                          "read container names in seg0");
		}

    }


	/** Encrypt a container.
     * @param t    the transaction that is used to configure the database 
     *             with new encryption properties.
     * @param ckey the key of the container that is being encrypted.
     * @return     file handle to the old copy  of the container.
     * @exception StandardException Standard Derby error policy
     */
	private StorageFile encryptContainer(RawTransaction  t, 
                                         ContainerKey    ckey)
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
        containerHdl.encryptContainer(newFilePath);
        containerHdl.close();

                    
        /*
         * Replace the current container file with the new container file after
         * keeping a copy of the current container file, it will be removed on 
         * post-commit or on a rollback this copy will be replace the container 
         * file to bring the database back to the state before encryption 
         * process started.  
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

        if (!currentFile.renameTo(oldFile)) {
                throw StandardException.
                    newException(SQLState.RAWSTORE_ERROR_RENAMING_FILE,
                                 currentFile, oldFile);
            }

        // now replace current container file with the new file. 
        if (!newFile.renameTo(currentFile)) {
            throw StandardException.
                newException(SQLState.RAWSTORE_ERROR_RENAMING_FILE,
                             newFile, currentFile);
                
        }

        return oldFile ;
    }

    
    /**
     * Get file handle to a container file that is used to keep 
     * temporary versions of the container file.  
     */
    private StorageFile getFile(ContainerKey containerId, boolean old) {
        String path = getFilePath(containerId, old);
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
        if (oldFile.exists()) {
            if (currentFile.exists()) {
                // rename the current container file to be the new file.
                if (!currentFile.renameTo(newFile)) {
                    throw StandardException.
                        newException(SQLState.RAWSTORE_ERROR_RENAMING_FILE,
                                     currentFile, newFile);
                }
            }

            if (!oldFile.renameTo(currentFile)) {
                throw StandardException.
                    newException(SQLState.RAWSTORE_ERROR_RENAMING_FILE,
                                 oldFile, currentFile);
            }
        }

        // if the new copy of the container file exists, remove it.
        if (newFile.exists()) {

            if (!newFile.delete())
                throw StandardException.newException(
                                                 SQLState.UNABLE_TO_DELETE_FILE, 
                                                 newFile);
        }
    }
}


/**
 * This is a helper class to remove old versions of the 
 * container files during  the post-commit of the transaction 
 * that is used to configure database with new encryption properties.
 */
class RemoveFiles implements Serviceable 
{
	private StorageFile filesToGo[];
    private int noFiles = 0 ;

	RemoveFiles(StorageFile filesToGo[], int size) {
        this.filesToGo = filesToGo;
        this.noFiles = size;
	}

	public int performWork(ContextManager context)
        throws StandardException  {
        
        for (int i = 0; i < noFiles; i++) {
            if (filesToGo[i].exists())
            {
                if (!filesToGo[i].delete())
                {
                    throw StandardException.newException(
                    SQLState.FILE_CANNOT_REMOVE_FILE, filesToGo[i]);
                }
            }
            
        }
        return Serviceable.DONE;
    }


	public boolean serviceASAP() {
		return false;
	}

    /**
     * delete the files immediately during the post commit.
     * @return true, this work needs to done on user thread. 
     */
	public boolean serviceImmediately()	{
		return true;
	}	
}
