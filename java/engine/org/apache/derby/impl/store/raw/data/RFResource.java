/*

   Derby - Class org.apache.derby.impl.store.raw.data.RFResource

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.FileResource;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.access.AccessFactoryGlobals;
import org.apache.derby.iapi.store.access.DatabaseInstant;

import org.apache.derby.io.StorageFactory;
import org.apache.derby.io.WritableStorageFactory;
import org.apache.derby.io.StorageFile;
import org.apache.derby.io.StorageRandomAccessFile;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class RFResource implements FileResource {

	protected final BaseDataFileFactory factory;

	public RFResource(BaseDataFileFactory dataFactory) {
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

			StorageFile directory = file.getParentDir();
            if (!directory.exists())
			{
                if (!directory.mkdirs())
                {
					throw StandardException.newException(
                            SQLState.FILE_CANNOT_CREATE_SEGMENT, directory);
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
	  @see FileResource#remove
	  @exception StandardException Oops
	  */
	public void remove(String name, long currentGenerationId, boolean purgeOnCommit)
		throws StandardException
	{
		if (factory.isReadOnly())
			throw StandardException.newException(SQLState.FILE_READ_ONLY);

			
		ContextManager cm = ContextService.getFactory().getCurrentContextManager();

		Transaction tran = 
            factory.getRawStoreFactory().findUserTransaction(
                cm, AccessFactoryGlobals.USER_TRANS_NAME);

		tran.logAndDo(privRemoveFileOperation(name, currentGenerationId, purgeOnCommit));

		if (purgeOnCommit) {

			Serviceable s = privRemoveFile(getAsFile(name, currentGenerationId));

			tran.addPostCommitWork(s);
		}
	}

	/**
	  @see FileResource#replace
	  @exception StandardException Oops
	  */
	public long replace(String name, long currentGenerationId, InputStream source, boolean purgeOnCommit)
		throws StandardException
	{
		if (factory.isReadOnly())
			throw StandardException.newException(SQLState.FILE_READ_ONLY);

		remove(name, currentGenerationId, purgeOnCommit);

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

	/**
	  @see FileResource#getAsFile
	  */
	public StorageFile getAsFile(String name)
	{
		return factory.storageFactory.newStorageFile( name);
	}

	/**
	  @see FileResource#getAsStream
	  @exception IOException trouble accessing file.
	  */
	public InputStream getAsStream(String name, long generationId) 
		 throws IOException
	{
        return getAsFile(name, generationId).getInputStream();
	}

	/**
	  @see FileResource#getAsStream
	  @exception IOException trouble accessing file.
	  */
	public InputStream getAsStream(String name)
		 throws IOException
	{
		return getAsFile(name).getInputStream();
	}

	/**
	  @see FileResource#purgeOldGenerations
	  */
    public void purgeOldGenerations(DatabaseInstant purgeTo)
	{

		// search from the start of the log until now
		// remove any generation files that have been
		// logged for removal and their transaction committed.

		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("purgeOldGenerations is not implemented");
	}

    public char getSeparatorChar()
    {
        return factory.storageFactory.getSeparator();
    }
    
    protected Serviceable privRemoveFile(StorageFile file)
    {
        return new RemoveFile(file);
    }

    protected RemoveFileOperation privRemoveFileOperation(
        String name, long generationId, boolean removeAtOnce)
    {
        return new RemoveFileOperation(name,generationId,removeAtOnce);
    }
} // end of class RFResource


class RemoveFile implements Serviceable
{
	private final StorageFile fileToGo;

	RemoveFile(StorageFile fileToGo)
    {
		this.fileToGo = fileToGo;
	}

	public int performWork(ContextManager context)
        throws StandardException
    {
        // SECURITY PERMISSION - MP1, OP5
        if (fileToGo.exists())
        {
            if (!fileToGo.delete())
            {
                throw StandardException.newException(
                    SQLState.FILE_CANNOT_REMOVE_FILE, fileToGo);
            }
        }
        return Serviceable.DONE;
	}

	public boolean serviceASAP()
    {
		return false;
	}


	// @return true, if this work needs to be done on a user thread immediately
	public boolean serviceImmediately()
	{
		return false;
	}	
}
