/*

   Derby - Class org.apache.derbyTesting.unitTests.store.T_RawStoreRow

   Copyright 1997, 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.unitTests.store;

import org.apache.derby.iapi.store.raw.*;

import org.apache.derby.iapi.store.access.*;
import org.apache.derby.iapi.types.SQLChar;

import org.apache.derby.iapi.types.DataValueDescriptor;

import java.io.*;

/* 
 * Implements a row of N columns of strings, or objects.
 * Used for testing raw store functionality.
 */
public class T_RawStoreRow {

	DataValueDescriptor[] col;

	public T_RawStoreRow(int numberOfColumns) {
		super();
		col = new DataValueDescriptor[numberOfColumns];
	}

	public T_RawStoreRow(String data) {
		this(1);
		col[0] = data == null ? new SQLChar() : new SQLChar(data);
	}

	public DataValueDescriptor[] getRow() {
		return col;
	}

	public void setColumn(int columnId, String data)
	{
		col[columnId] = data == null ? new SQLChar() : new SQLChar(data);
	}

	public void setColumn(int columnId, int stringLen, String data)
	{
		// in store it will take (stringLen * 2) bytes
		col[columnId] = new SQLChar(T_Util.getStringFromData(data, stringLen));
	}

	public void setColumn(int columnId, DataValueDescriptor data)
	{
		col[columnId] = data;
	}

	public DataValueDescriptor getStorableColumn(int columnId) {

		return col[columnId];
	}

	public DataValueDescriptor getColumn(int columnId) {

		return col[columnId];
	}

	public void setStorableColumn(int columnId, DataValueDescriptor value) {

		col[columnId] = value;
	}

	public  int nColumns() {
		return col.length;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer("");
		for (int i = 0; i < nColumns(); i++) {
			sb.append(col[i].toString());
			if (i < (nColumns() - 1))
				sb.append(",");
		}
		sb.append("");

		return sb.toString();
	}
}

