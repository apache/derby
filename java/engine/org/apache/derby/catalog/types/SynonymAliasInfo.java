/*

   Derby - Class org.apache.derby.catalog.types.SynonymAliasInfo

   Copyright 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.catalog.types;

import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.catalog.AliasInfo;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Describe an S (Synonym) alias.
 *
 * @see AliasInfo
 */
public class SynonymAliasInfo implements AliasInfo, Formatable
{
	private String schemaName = null;
	private String tableName = null;

	public SynonymAliasInfo() {
	}

	/**
		Create a SynonymAliasInfo for synonym.
	*/
	public SynonymAliasInfo(String schemaName, String tableName)
	{
		this.schemaName = schemaName;
		this.tableName = tableName;
	}

	public String getSynonymTable() {
		return tableName;
	}

	public String getSynonymSchema() {
		return schemaName;
	}

	// Formatable methods

	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
	public void readExternal( ObjectInput in )
		 throws IOException, ClassNotFoundException
	{
		schemaName = (String) in.readObject();
		tableName = (String) in.readObject();
	}

	/**
	 * Write this object to a stream of stored objects.
	 *
	 * @param out write bytes here.
	 *
	 * @exception IOException		thrown on error
	 */
	public void writeExternal( ObjectOutput out )
		 throws IOException
	{
		out.writeObject(schemaName);
		out.writeObject(tableName);
	}
 
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.SYNONYM_INFO_V01_ID; }

	public String toString() {
		return "\"" + schemaName + "\".\"" + tableName + "\"";
	}

	public String getMethodName()
	{
		return null;
	}
}

