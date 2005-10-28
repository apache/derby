/*

   Derby - Class org.apache.derby.impl.store.raw.data.ContainerBasicOperation

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

import org.apache.derby.iapi.store.raw.ContainerKey;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.data.RawContainerHandle;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.CompressedNumber;
import org.apache.derby.iapi.util.ByteArray;

import java.io.OutputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.IOException;

/** 
A Container Operation change the state of the container.
A ContainerBasicOperation is the base class for all container operations.
*/

public abstract class ContainerBasicOperation implements Loggable
{
	/* page info this operation changed */
	private long containerVersion;
	protected ContainerKey containerId;

	transient protected RawContainerHandle containerHdl = null;
	transient private boolean foundHere = false;

	protected ContainerBasicOperation(RawContainerHandle hdl) throws StandardException
	{
		containerHdl = hdl;
		containerId = hdl.getId();
		containerVersion = hdl.getContainerVersion();
	}

	/*
	 * Formatable methods
	 */

	// no-arg constructor, required by Formatable
	public ContainerBasicOperation() { super(); }

	public void writeExternal(ObjectOutput out) throws IOException
	{
		containerId.writeExternal(out);
		CompressedNumber.writeLong(out, containerVersion);
	}

	public void readExternal(ObjectInput in) 
		 throws IOException, ClassNotFoundException 
	{
		containerId = ContainerKey.read(in);
		containerVersion = CompressedNumber.readLong(in);
	}


	/**
		Loggable methods
	*/

	/**
		the default for prepared log is always null for all the operations
		that don't have optionalData.  If an operation has optional data,
		the operation need to prepare the optional data for this method.

		Space Operation has no optional data to write out
	*/
	public ByteArray getPreparedLog()
	{
		return (ByteArray) null;
	}

	public void releaseResource(Transaction tran)
	{
		if (!foundHere)
			return;

		if (containerHdl != null)
		{
			containerHdl.close();
			containerHdl = null;
		}

		foundHere = false;
	}

	/**
		A space operation is a RAWSTORE log record
	*/
	public int group()
	{
		return Loggable.RAWSTORE;
	}

	/**
		Methods specific to this class
	*/

	/**
	  Open the container with this segmentId and containerId.
	  This method should only be called if the container has already been
	  created.

	  @exception StandardException the container cannot be found or cannot be
	  opened.
	 */
	protected RawContainerHandle findContainer(Transaction tran)
		 throws StandardException
	{
		releaseResource(tran);

		RawTransaction rtran = (RawTransaction)tran;
		containerHdl = rtran.openDroppedContainer(
			containerId, (LockingPolicy) null);

		//If we are in roll forward recovery, missing container will be
		//recreated becuase we might have hit a log record which has a 
		//reused the container id that was dropped earlier.
		if (rtran.inRollForwardRecovery())
		{
			if (containerHdl == null)
			{
				if (SanityManager.DEBUG) 
					if(SanityManager.DEBUG_ON("LoadTran"))
						SanityManager.DEBUG_PRINT("Trace", "cannot find container " + containerId + 
												  ", now attempt last ditch effort");
				

				containerHdl = findContainerForLoadTran(rtran);

				if (SanityManager.DEBUG) 
					if(SanityManager.DEBUG_ON("LoadTran"))
						SanityManager.DEBUG_PRINT("Trace",
												  " findContainerForLoadTran, got container=" +
												  (containerHdl != null));

			}
		}	
        
		if (containerHdl == null)
        {
			throw StandardException.newException(
                    SQLState.DATA_CONTAINER_VANISHED, containerId);
        }

		foundHere = true;
		return containerHdl;
	}

	/**
		Subclass (e.g., ContainerOperation) that wishes to do something abou
		missing container in load tran should override this method to return
		the recreated container

		@exception StandardException Cloudscape Standard error policy
	 */
	protected RawContainerHandle findContainerForLoadTran(RawTransaction tran) 
		 throws StandardException 
	{
		return null;
	}
	

	/**
		@exception StandardException Standard Cloudscape error policy
	*/
	public boolean needsRedo(Transaction xact)
		 throws StandardException
	{
		findContainer(xact);

		long cVersion = containerHdl.getContainerVersion();

		if (cVersion == containerVersion)
			return true;

		releaseResource(xact);

		if (cVersion > containerVersion)
			return false;
		else
		{
			// RESOLVE - correct error handling
            if (SanityManager.DEBUG)
            {
    			SanityManager.THROWASSERT("log corrupted, missing log record: "+
										  "log container version = " +
										  containerVersion + 
										  " container header version " + cVersion);
    		}
			return false;
		}
	}


	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "Space Operation: " + containerId ;
		}
		else
			return null;
	}

}


