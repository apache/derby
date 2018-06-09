/*

   Derby - Class org.apache.derby.catalog.types.UDTAliasInfo

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

package org.apache.derby.catalog.types;

import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.catalog.AliasInfo;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Describe an A (Abstract Data Type) alias. For the first release of USer
 * Defined Types, this is a vacuous object. Future revs may add real information
 * to this object. The UDTAliasInfo maintains a version stamp so that it
 * can evolve its persistent form over time.
 *
 * @see AliasInfo
 */
public class UDTAliasInfo implements AliasInfo, Formatable
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static final int FIRST_VERSION = 0;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    // no-arg constructor for Formatable machinery
	public UDTAliasInfo() {}

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // AliasInfo BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

	public boolean isTableFunction() {return false; }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // Formatable BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

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
        // as the persistent form evolves, switch on this value
        int oldVersion = in.readInt();
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
		out.writeInt( FIRST_VERSION );
	}
 
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.UDT_INFO_V01_ID; }

    /**
     * This is used by dblook to reconstruct the UDT-specific parts of the ddl
     * needed to recreate this alias.
     */
	public String toString() {
		return "LANGUAGE JAVA";
	}

	public String getMethodName()
	{
		return null;
	}
}

