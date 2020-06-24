/*

   Derby - Class org.apache.derby.impl.store.raw.data.InputStreamContainer

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

import org.apache.derby.impl.store.raw.data.FileContainer;

import org.apache.derby.iapi.store.raw.ContainerKey;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.impl.store.raw.data.BaseDataFileFactory;

import org.apache.derby.iapi.services.io.InputStreamUtil;
import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.io.StorageFile;

import java.io.InputStream;
import java.io.IOException;
import java.io.DataInputStream;

/**
	A class that uses a ZipEntry to be a single container file,
	but read-only.

*/

final class InputStreamContainer extends FileContainer  {
//IC see: https://issues.apache.org/jira/browse/DERBY-467

    private StorageFile containerPath;
    
	/*
	 * Constructors
	 */

//IC see: https://issues.apache.org/jira/browse/DERBY-467
	InputStreamContainer(BaseDataFileFactory factory) {
		super(factory);
		canUpdate = false;
	}

	final boolean openContainer(ContainerKey newIdentity) throws StandardException {
		DataInputStream dis = null;
		try
        {
			InputStream is = null;
            containerPath = dataFactory.getContainerPath(newIdentity, false);
            try
            {
                is = containerPath.getInputStream();
            }
            catch (IOException ioe)
            {
                // Maybe it's been stubbified.
                containerPath = dataFactory.getContainerPath(newIdentity, true);
                try
                {
                    is = getInputStream();
                }
                catch (IOException ioe2)
                {
                    containerPath = null;
                    return false;
                }
            }

			dis = new DataInputStream(is);
			
			// FileData has to be positioned just at the beginning 
			// of the first allocation page. And it is because we
			// just opened the stream and the first allocation page
			// is located at the beginning of the file.
			readHeader(getEmbryonicPage(dis));
//IC see: https://issues.apache.org/jira/browse/DERBY-3347

			return true;

        } catch (IOException ioe) {
            throw StandardException.
//IC see: https://issues.apache.org/jira/browse/DERBY-1958
                newException(SQLState.FILE_CONTAINER_EXCEPTION, 
                             ioe,
                             new Object[] {getIdentity().toString(),
                                           "open", newIdentity.toString()});
        } finally {
			if (dis != null) {
				try {
					dis.close();
				} catch (IOException ioe) {}
			}
		}
	} // end of openContainer

//IC see: https://issues.apache.org/jira/browse/DERBY-467
	void closeContainer()
    {
		containerPath = null;
	}

	/**
		Write out the header information for this container. If an i/o exception
		occurs then ...

		@see org.apache.derby.iapi.services.cache.Cacheable#clean
		@exception StandardException Standard Derby error policy
	*/
	public final void clean(boolean forRemove) throws StandardException {

		// Nothing to do since we are inherently read-only.

	}

	/**
		Preallocate page.  
	*/
	protected final int preAllocate(long lastPreallocPagenum, int preAllocSize) {

		// Nothing to do since we are inherently read-only.
		return 0;
	}

	protected void truncatePages(long lastValidPagenum)
    {
		// Nothing to do since we are inherently read-only.
//IC see: https://issues.apache.org/jira/browse/DERBY-132
		return;
    }
    

	/*
	** Container creation, opening, and closing
	*/

	/**
		Create a new container, all references to identity must be through the
		passed in identity, this object will no identity until after this method returns.
	*/
//IC see: https://issues.apache.org/jira/browse/DERBY-467
	void createContainer(ContainerKey newIdentity) throws StandardException {
		// RESOLVE - probably should throw an error ...
	}



	/**
		Remove the container.
	*/
	protected final void removeContainer(LogInstant instant, boolean leaveStub) throws StandardException 
	{
	// RESOVE
	}

	/*
	** Methods used solely by StoredPage
	*/

	/**
		Read a page into the supplied array.

		<BR> MT - thread safe
	*/
	protected final void readPage(long pageNumber, byte[] pageData) 
		 throws IOException, StandardException
	{

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(!getCommittedDropState());
		}

		long pageOffset = pageNumber * pageSize;

		readPositionedPage(pageOffset, pageData);

		if (dataFactory.databaseEncrypted() && 
			pageNumber != FIRST_ALLOC_PAGE_NUMBER)
		{
			decryptPage(pageData, pageSize);
		}
	}

	/**
		Read the page at the positioned offset.
		This default implementation, opens the stream and skips to the offset
		and then reads the data into pageData.
	*/
	protected void readPositionedPage(long pageOffset, byte[] pageData) throws IOException {


		InputStream is = null;
		try {
			// no need to synchronize as each caller gets a new stream
			is = getInputStream();

			InputStreamUtil.skipFully(is, pageOffset);
//IC see: https://issues.apache.org/jira/browse/DERBY-3770

			InputStreamUtil.readFully(is, pageData, 0, pageSize);

			is.close();
			is = null;
		} finally {
			if (is != null) {
				try {is.close();} catch (IOException ioe) {}
			}
		}
	}

	/**
		Write a page from the supplied array.

		<BR> MT - thread safe
	*/
	protected final void writePage(long pageNumber, byte[] pageData, boolean syncPage)
		throws IOException, StandardException {
	}

	protected final void flushAll() {
	}

	/**
		Get an input stream positioned at the beginning of the file
	*/
	protected InputStream getInputStream() throws IOException
    {
        return containerPath.getInputStream();
    }

		
	/**
     * Backup the container.
     * There is no support to backup this type of containers. It may not be any
     * real use for users because users can simply  make copies of the read only 
     * database in Zip files easily using OS utilities.
     * 
     * @exception StandardException Standard Derby error policy 
     */
	protected void backupContainer(BaseContainerHandle handle,	String backupLocation)
	    throws StandardException
	{
        throw StandardException.newException(
                SQLState.STORE_FEATURE_NOT_IMPLEMENTED);
	}


    /**
     * Encrypts or decrypts the container.
     * <p>
     * These operations are unsupported for this type of container.
     *
     * @throws StandardException STORE_FEATURE_NOT_IMPLEMENTED
     */
	protected void encryptOrDecryptContainer(BaseContainerHandle handle,
//IC see: https://issues.apache.org/jira/browse/DERBY-5792
                                             String newFilePath,
                                             boolean doEncrypt)
	    throws StandardException
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-239
        throw StandardException.newException(
                SQLState.STORE_FEATURE_NOT_IMPLEMENTED);
	}

}
