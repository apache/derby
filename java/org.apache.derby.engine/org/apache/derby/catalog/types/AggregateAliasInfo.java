/*

   Derby - Class org.apache.derby.catalog.types.AggregateAliasInfo

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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.catalog.TypeDescriptor;

/**
 * Describe a G (Aggregate) alias. The AggregateAliasInfo maintains a version stamp so that it
 * can evolve its persistent form over time.
 *
 * @see AliasInfo
 */
public class AggregateAliasInfo implements AliasInfo, Formatable
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

    private TypeDescriptor  _forType;
    private TypeDescriptor  _returnType;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** no-arg constructor for Formatable machinery */
	public AggregateAliasInfo() {}

    /** Construct from pieces */
    public  AggregateAliasInfo
        (
         TypeDescriptor forType,
         TypeDescriptor returnType
         )
    {
        _forType = forType;
        _returnType = returnType;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // AliasInfo BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

	public boolean isTableFunction() { return false; }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ACCESSORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

	public TypeDescriptor   getForType() { return _forType; }
    public TypeDescriptor   getReturnType() { return _returnType; }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // BIND TIME LOGIC
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Set the collation type for string input and return types.
     */
    public void setCollationTypeForAllStringTypes( int collationType )
    {
        _forType = DataTypeDescriptor.getCatalogType( _forType, collationType );
        _returnType = DataTypeDescriptor.getCatalogType( _returnType, collationType );
    }
    
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

        _forType = (TypeDescriptor) in.readObject();
        _returnType = (TypeDescriptor) in.readObject();
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

        out.writeObject( _forType );
        out.writeObject( _returnType );
	}
 
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.AGGREGATE_INFO_V01_ID; }

    /**
     * This is used by dblook to reconstruct the aggregate-specific parts of the ddl
     * needed to recreate this alias.
     */
	public String toString() {
		return "FOR " + _forType.getSQLstring() +
            " RETURNS " + _returnType.getSQLstring();
	}

	public String getMethodName()
	{
		return null;
	}
}

