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

import org.apache.derby.iapi.store.raw.*;

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.util.ByteArray;
import java.io.IOException;
import java.io.OutputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.derby.iapi.services.io.LimitObjectInput;

public class T_Compensation
implements Compensation
{
	// no-arg constructor, required by Formatable 
	public T_Compensation() { super(); }


	/*
	  Loggable methods
	  */
	public void doMe(Transaction xact, LogInstant instant,
					 LimitObjectInput in)
	{
		//System.out.println("Loggable.doMe("+toString()+")");
		return;
	}

	/*
		methods to support prepared log
		the following two methods should not be called during recover
	*/

	public ByteArray getPreparedLog()
	{ return (ByteArray) null; }

	public boolean needsRedo(Transaction xact) {return false;}
	public void releaseResource(Transaction xact) {return;}
	public int group() { return Loggable.COMPENSATION | Loggable.RAWSTORE; }

	/*
	  Compensation methods.
	  */
	public void setUndoOp(Undoable op) {return;}

	/*
	  Formatable methods
	  */
	public void writeExternal(ObjectOutput out)
	throws IOException
	{return;}

	public void readExternal(ObjectInput in) 
	throws IOException,ClassNotFoundException
	{return;}

	public int getTypeFormatId()
	{
		return StoredFormatIds.SERIALIZABLE_FORMAT_ID;
	}
}
