/*

   Derby - Class org.apache.derby.impl.store.raw.data.LongColumnException

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

import org.apache.derby.iapi.error.StandardException; 

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream;

/**
	An exception used to pass a specfic "error code" through
	various layers of software.
*/
public class LongColumnException extends StandardException 
{

	// YYZ? may need to make it a DynamicByteArrayOutputStream, or a ByteArray
	protected DynamicByteArrayOutputStream    logBuffer;
	protected int                       nextColumn;
	protected int                       realSpaceOnPage;
	protected Object                    column;

	/*
	** Constructor
	*/
	public LongColumnException() {
		super("lngcl.U");
	}

	public void setColumn(Object column) {
		this.column = column;
	}

	public void setExceptionInfo(DynamicByteArrayOutputStream out,
			int nextColumn, int realSpaceOnPage) {
		this.logBuffer = out;
		this.nextColumn = nextColumn;
		this.realSpaceOnPage = realSpaceOnPage;

		// buffer length can be calculated:
		// out.getPosition() - out.getBeginPosition()
	}

	public Object getColumn() {
		return this.column;
	}

	public DynamicByteArrayOutputStream getLogBuffer() {
		return this.logBuffer;
	}

	public int getNextColumn() {
		return this.nextColumn;
	}

	public int getRealSpaceOnPage() {
		return this.realSpaceOnPage;
	}
}
