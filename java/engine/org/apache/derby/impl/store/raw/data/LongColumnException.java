/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.raw.data
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
	/**
		IBM Copyright &copy notice.
	*/
 
    private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

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
