/*

   Derby - Class org.apache.derby.impl.store.raw.data.RFResource

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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.store.access.AccessFactoryGlobals;
import org.apache.derby.iapi.store.access.FileResource;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.io.StorageFile;

class RFResource implements FileResource {

	private final BaseDataFileFactory factory;

	RFResource(BaseDataFileFactory dataFactory) {
		this.factory = dataFactory;
	}

	/**
	  @see FileResource#add
	  @exception StandardException Oops
	*/
	public long add(String name, InputStream source)
		 throws StandardException
	{
		OutputStream os = null;

		if (factory.isReadOnly())
        {
			throw StandardException.newException(SQLState.FILE_READ_ONLY);
        }

		long generationId = factory.getNextId();

		try
		{
			StorageFile file = getAsFile(name, generationId);
            if (file.exists())
            {
				throw StandardException.newException(
                        SQLState.FILE_EXISTS, file);
            }

            ContextManager cm = 
                ContextService.getFactory().getCurrentContextManager();

            RawTransaction tran = 
                factory.getRawStoreFactory().getXactFactory().findUserTransaction(
                        factory.getRawStoreFactory(), 
                        cm, 
                        AccessFactoryGlobals.USER_TRANS_NAME);
            
            // Block the backup, If backup is already in progress wait 
            // for the backup to finish. Jar files are unlogged but the 
            // changes to the  references to the jar file in the catalogs 
            // is logged. A consistent backup can not be made when jar file 
            // is being added.

            tran.blockBackup(true);

			StorageFile directory = file.getParentDir();
            StorageFile parentDir = directory.getParentDir();
            boolean pdExisted = parentDir.exists();

            if (!directory.exists())
			{
                if (!directory.mkdirs())
                {
					throw StandardException.newException(
                            SQLState.FILE_CANNOT_CREATE_SEGMENT, directory);
                }

                directory.limitAccessToOwner();

                if (!pdExisted) {
                    parentDir.limitAccessToOwner();
                }
			}

            os = file.getOutputStream();
			byte[] data = new byte[4096];
			int len;

			factory.writeInProgress();
			try
			{
				while ((len = source.read(data)) != -1) {
					os.write(data, 0, len);
				}
                factory.writableStorageFactory.sync( os, false);
			}
			finally
			{
				factory.writeFinished();
			}
		}

		catch (IOException ioe)
		{
			throw StandardException.newException(
                    SQLState.FILE_UNEXPECTED_EXCEPTION, ioe);
		}

		finally
		{
			try {
				if (os != null) {
					os.close();
				}
			} catch (IOException ioe2) {/*RESOLVE: Why ignore this?*/}

			try {
				if (source != null)source.close();
			} catch (IOException ioe2) {/* RESOLVE: Why ignore this?*/}
		}
		
		return generationId;
	}

    /**
     * @see FileResource#removeJarDir
     */
    public void removeJarDir(String f) throws StandardException {
        if (factory.isReadOnly())
            throw StandardException.newException(SQLState.FILE_READ_ONLY);

        ContextManager cm =
            ContextService.getFactory().getCurrentContextManager();

        RawTransaction tran =
            factory.getRawStoreFactory().getXactFactory().findUserTransaction(
                        factory.getRawStoreFactory(),
                        cm,
                        AccessFactoryGlobals.USER_TRANS_NAME);

        StorageFile ff = factory.storageFactory.newStorageFile(f);
        Serviceable s = new RemoveFile(ff);

        // Since this code is only used during upgrade to post-10.8 databases
        // we do no bother to build code for a special RemoveDirOperation and
        // do tran.logAndDo (cf. logic in #remove). If the post-commit removal
        // doesn't get completed, that is no big issue, the dirs can be removed
        // by hand if need be. A prudent DBA will rerun the upgrade from a
        // backup if something crashes anyway..

        tran.addPostCommitWork(s);
    }

    /**
	  @see FileResource#remove
	  @exception StandardException Oops
	  */
	public void remove(String name, long currentGenerationId)
		throws StandardException
	{
		if (factory.isReadOnly())
			throw StandardException.newException(SQLState.FILE_READ_ONLY);

			
		ContextManager cm = ContextService.getFactory().getCurrentContextManager();

        RawTransaction tran = 
            factory.getRawStoreFactory().getXactFactory().findUserTransaction(
                        factory.getRawStoreFactory(), 
                        cm, 
                        AccessFactoryGlobals.USER_TRANS_NAME);
                    
        // Block the backup, If backup is already in progress wait 
        // for the backup to finish. Jar files are unlogged but the 
        // changes to the  references to the jar file in the catalogs 
        // is logged. A consistent backup can not be made when jar file 
        // is being removed.

        tran.blockBackup(true);

		tran.logAndDo(new RemoveFileOperation(name, currentGenerationId, true));

		Serviceable s = new RemoveFile(getAsFile(name, currentGenerationId));

	    tran.addPostCommitWork(s);
	}

	/**
	  @see FileResource#replace
	  @exception StandardException Oops
	  */
	public long replace(String name, long currentGenerationId, InputStream source)
		throws StandardException
	{
		if (factory.isReadOnly())
			throw StandardException.newException(SQLState.FILE_READ_ONLY);

		remove(name, currentGenerationId);

		long generationId = add(name, source);

		return generationId;
	}


	/**
	  @see FileResource#getAsFile
	  */
	public StorageFile getAsFile(String name, long generationId)
	{
		String versionedFileName = factory.getVersionedName(name, generationId);

		return factory.storageFactory.newStorageFile( versionedFileName);
	}

    public char getSeparatorChar()
    {
        return factory.storageFactory.getSeparator();
    }
} // end of class RFResource


final class RemoveFile implements Serviceable, PrivilegedExceptionAction<Object>
{
	private final StorageFile fileToGo;

	RemoveFile(StorageFile fileToGo)
    {
		this.fileToGo = fileToGo;
	}

	public int performWork(ContextManager context)
        throws StandardException
    {
        try {
            AccessController.doPrivileged(this);
        } catch (PrivilegedActionException e) {
            throw (StandardException) (e.getException());
         }
        return Serviceable.DONE;
	}

	public boolean serviceASAP()
    {
		return false;
	}

    /**
     * File deletion is a quick operation and typically releases substantial
     * amount of space very quickly, this work should be done on the
     * user thread. 
     * @return true, this work needs to done on user thread. 
     */
	public boolean serviceImmediately()
	{
		return true;
	}

    public Object run() throws StandardException {
        // SECURITY PERMISSION - MP1, OP5
        if (fileToGo.exists()) {
            if (fileToGo.isDirectory()) {
                if (!fileToGo.deleteAll()) {
                    throw StandardException.newException(
                            SQLState.FILE_CANNOT_REMOVE_JAR_FILE, fileToGo);
                }
            } else {
                if (!fileToGo.delete()) {
                    throw StandardException.newException(
                            SQLState.FILE_CANNOT_REMOVE_JAR_FILE, fileToGo);
                }
            }
        }
        return null;
    }	
}
