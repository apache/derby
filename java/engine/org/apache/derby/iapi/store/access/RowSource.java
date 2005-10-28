/*

   Derby - Class org.apache.derby.iapi.store.access.RowSource

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

package org.apache.derby.iapi.store.access;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.io.FormatableBitSet;

/**

  A RowSource is the mechanism for iterating over a set of rows.  The RowSource
  is the interface through which access recieved a set of rows from the client
  for the purpose of inserting into a single conglomerate.

  <p>
  A RowSource can come from many sources - from rows that are from fast path
  import, to rows coming out of a sort for index creation.

*/ 
public interface RowSource {

	/**
		Get the next row as an array of column objects. The column objects can
		be a JBMS Storable or any
		Serializable/Externalizable/Formattable/Streaming type.
		<BR>
		A return of null indicates that the complete set of rows has been read.

		<p>
		A null column can be specified by leaving the object null, or indicated
		by returning a non-null getValidColumns.  On streaming columns, it can
		be indicated by returning a non-null get FieldStates.

		<p>
        If RowSource.needToClone() is true then the returned row 
        (the DataValueDescriptor[]) is guaranteed not to be modified by drainer
        of the RowSource (except that the input stream will be read, of course) 
        and drainer will keep no reference to it before making the subsequent 
        nextRow call.  So it is safe to return the same DataValueDescriptor[] 
        in subsequent nextRow calls if that is desirable for performance 
        reasons.  

		<p>
        If RowSource.needToClone() is false then the returned row (the 
        DataValueDescriptor[]) may be be modified by drainer of the RowSource, 
        and the drainer may keep a reference to it after making the subsequent 
        nextRow call.  In this case the client should severe all references to 
        the row after returning it from getNextRowFromRowSource().

		@exception StandardException Cloudscape Standard Error Policy
	 */
	public DataValueDescriptor[] getNextRowFromRowSource() 
        throws StandardException;

	/**
        Does the caller of getNextRowFromRowSource() need to clone the row
        in order to keep a reference to the row past the 
        getNextRowFromRowSource() call which returned the row.  This call
        must always return the same for all rows in a RowSource (ie. the
        caller will call this once per scan from a RowSource and assume the
        behavior is true for all rows in the RowSource).

	 */
	public boolean needsToClone();

	/**
	  getValidColumns describes the DataValueDescriptor[] returned by all calls
      to the getNextRowFromRowSource() call. 

	  If getValidColumns returns null, the number of columns is given by the
	  DataValueDescriptor.length where DataValueDescriptor[] is returned by the
      preceeding getNextRowFromRowSource() call.  Column N maps to 
      DataValueDescriptor[N], where column numbers start at zero.

	  If getValidColumns return a non null validColumns FormatableBitSet the number of
	  columns is given by the number of bits set in validColumns.  Column N is
	  not in the partial row if validColumns.get(N) returns false.  Column N is
	  in the partial row if validColumns.get(N) returns true.  If column N is
	  in the partial row then it maps to DataValueDescriptor[M] where M is the 
      count of calls to validColumns.get(i) that return true where i < N.  If
	  DataValueDescriptor.length is greater than the number of columns 
      indicated by validColumns the extra entries are ignored.  
	*/
	FormatableBitSet getValidColumns(); 

	/**
		closeRowSource tells the RowSource that it will no longer need to
		return any rows and it can release any resource it may have.
		Subsequent call to any method on the RowSource will result in undefined
		behavior.  A closed rowSource can be closed again.
	*/
	void closeRowSource();
}
