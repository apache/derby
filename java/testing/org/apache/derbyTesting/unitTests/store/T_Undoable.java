/*

   Derby - Class org.apache.derbyTesting.unitTests.store.T_DaemonService

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

package org.apache.derbyTesting.unitTests.store;

import org.apache.derbyTesting.unitTests.harness.T_Fail;

import org.apache.derby.iapi.store.raw.*;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.util.ByteArray;
import org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream;
import java.io.IOException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.StreamCorruptedException;
import java.util.Vector;
import java.io.OutputStream;
import java.io.InputStream;
import org.apache.derby.iapi.services.io.LimitObjectInput;

//
// Tracing can be done like so (commented out)
//import org.apache.derbyTesting.unitTests.util.MsgTrace;

public class T_Undoable
implements Undoable
{
	static final int REMOVE_NONE  = 0;
	static final int REMOVE_ME    = 1;
	static final int REMOVE_TO_ME = 2;

	protected ByteArray preparedLog;
	protected DynamicByteArrayOutputStream logBuffer;

	GlobalTransactionId tid = null;
	int tranSeq = -1;
	int recordSeq = -1;
	int removeWhat = REMOVE_NONE;
	int LWMTranSeq = 0;
	boolean rollBack = true;
	int lastRecord = -1;
	boolean lastTransaction = false;
	int optionalDataLen;
	boolean verbose;

	// no-arg constructor, required by Formatable 
	public T_Undoable() { super(); }


	T_Undoable(GlobalTransactionId tid, int tranSeq, int recordSeq,
			   int removeWhat, int LWMTranSeq,
			   boolean rollBack, int lastRecord, boolean lastTransaction,
			   int optionalDataLen,boolean verbose)
		 throws T_Fail
	{
//MsgTrace.traceString("{{{tu.new");
		T_Fail.T_ASSERT((removeWhat >= REMOVE_NONE) &&
							 (removeWhat <= REMOVE_TO_ME));
		T_Fail.T_ASSERT(rollBack == (recordSeq < 0));
		T_Fail.T_ASSERT(rollBack == (tranSeq < 0));
		this.tid = tid;
		this.tranSeq = tranSeq;
		this.recordSeq = recordSeq;
		this.removeWhat = removeWhat;
		this.LWMTranSeq = LWMTranSeq;
		this.rollBack = rollBack;
		this.lastRecord = lastRecord;
		this.lastTransaction = lastTransaction;
		this.optionalDataLen = optionalDataLen;
		this.verbose = verbose;
		
		try {
			writeOptionalDataToBuffer();
		} catch (IOException ioe) {
			throw T_Fail.exceptionFail(ioe);
		} catch (StandardException se) {
			throw T_Fail.exceptionFail(se);
		}

//MsgTrace.traceString("}}}tu.new");

	}
	
	private void writeOptionalDataToBuffer()
		throws StandardException, IOException
	{

		if (logBuffer == null) {
			// YYZ: need to revisit this.  Do we really want to allocate this much for a buffer every time?
			logBuffer = new DynamicByteArrayOutputStream(1024); // init size 1K
		} else {
			logBuffer.reset();
		}

		int optionalDataStart = logBuffer.getPosition();

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(optionalDataStart == 0,
				"Buffer for writing the optional data should start at position 0");
		}

//MsgTrace.traceString("{{{tu.writeOpetionalData");
		if (optionalDataLen > 0)
		{
			byte[] buf = new byte[optionalDataLen];
			for (int ix=0;ix <optionalDataLen;ix++)
				buf[ix] = (byte)ix;
			logBuffer.write(buf);
		}
//MsgTrace.traceString("}}}tu.writeOpetionalData");

		int optionalDataLength = logBuffer.getPosition() - optionalDataStart;
		
		if (SanityManager.DEBUG) {
			if (optionalDataLength != logBuffer.getUsed())
				SanityManager.THROWASSERT("wrong optional data length, optionalDataLength = "
					+ optionalDataLength + ", logBuffer.getUsed() = " + logBuffer.getUsed());
		}

		// set the position to the beginning of the buffer
		logBuffer.setPosition(optionalDataStart);

		this.preparedLog = new ByteArray (logBuffer.getByteArray(), optionalDataStart,
			optionalDataLength);
	}
	
	/*
	  Loggable methods
	  */
	public void doMe(Transaction xact, LogInstant instant,
					 LimitObjectInput in)
	{
		if (verbose)
			System.out.println("Loggable.doMe("+toString()+")");
		return;
	}

	/*
		methods to support prepared log
		the following two methods should not be called during recover
	*/

	public ByteArray getPreparedLog()
	{
		return this.preparedLog;
	}

	public boolean needsRedo(Transaction xact) {return false;}
	public void releaseResource(Transaction xact) {return;}
	public int group () { return Loggable.RAWSTORE ; };

	/*
	  Undoable methods.
	 */
	public Compensation generateUndo(Transaction xact, LimitObjectInput in)
		 throws StandardException, IOException
	{
//MsgTrace.traceString("+++tu.generateUndo");
		return new T_Compensation();
	}

	/*
	  Formatable methods
	  */

	/**
	 @exception IOException	thrown on error
	 */
	public void writeExternal(ObjectOutput out)
	throws IOException
	{
//MsgTrace.traceString("{{{tu.writeExternal");
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT((removeWhat >= REMOVE_NONE) &&
								 (removeWhat <= REMOVE_TO_ME));
			SanityManager.ASSERT(rollBack == (recordSeq < 0));
			SanityManager.ASSERT(rollBack == (tranSeq < 0));
		}
		out.writeObject(tid);
		out.writeInt(tranSeq);
		out.writeInt(recordSeq);
		out.writeInt(removeWhat);
		out.writeInt(LWMTranSeq);
		out.writeBoolean(rollBack);
		out.writeInt(lastRecord);
		out.writeBoolean(lastTransaction);
		out.writeInt(optionalDataLen);
//MsgTrace.traceString("}}}tu.writeExternal");
	}

	public void readExternal(ObjectInput in) 
	throws IOException,ClassNotFoundException
	{
//MsgTrace.traceString("{{{tu.readExternal");
		try
		{
			tid = (GlobalTransactionId)in.readObject();
			tranSeq = in.readInt();
			recordSeq = in.readInt();
			removeWhat = in.readInt();
			LWMTranSeq = in.readInt();
			rollBack = in.readBoolean();
			lastRecord = in.readInt();
			lastTransaction = in.readBoolean();
			optionalDataLen = in.readInt();
		}

		catch ( ClassCastException exception ) {
//MsgTrace.traceString("{{{tu.readExternal---exception");
			throw new StreamCorruptedException();
		}
//MsgTrace.traceString("}}}tu.readExternal");
	}

	public int getTypeFormatId()
	{
		return StoredFormatIds.SERIALIZABLE_FORMAT_ID;
	}

	/*
	  Object methods.
	  */
	public String toString()
	{
	    String traceTid = "tid: null";

		if (tid !=null) traceTid = "tid: "+tid;

		String traceRemoveWhat;
		switch (removeWhat)
		{
		case  REMOVE_NONE:
			traceRemoveWhat = "REMOVE_NONE";
			break;
		case  REMOVE_ME:
			traceRemoveWhat = "REMOVE_ME";
			break;
		case  REMOVE_TO_ME:
			traceRemoveWhat = "REMOVE_TO_ME";
			break;
		default:
			traceRemoveWhat = "removeWhat: invalidValue";
			break;
		}

		return
			traceTid+" "+
			"tranSeq: "+tranSeq+" "+
			"recordSeq: "+recordSeq+" "+
			traceRemoveWhat+" "+
			"LWMTranSeq: "+LWMTranSeq+" "+
			"rollback: "+rollBack+" "+
			"lastRecord: "+lastRecord+" "+
			"optionalDataLen: "+optionalDataLen+" "+
			"lastTransaction: "+lastTransaction;
	}

}
