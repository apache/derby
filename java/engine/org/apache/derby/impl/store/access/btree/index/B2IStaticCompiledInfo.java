/*

   Derby - Class org.apache.derby.impl.store.access.btree.index.B2IStaticCompiledInfo

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.store.access.btree.index;

import org.apache.derby.iapi.services.io.ArrayInputStream;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds; 

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;

import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.types.DataValueDescriptor;


import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**

This class implements the static compiled information relevant to a btree
secondary index.  It is what is returned by 
B2I.getStaticCompiledOpenConglomInfo().
<p>
Currently the only interesting information stored is Conglomerate for this
index and the Conglomerate for the base table of this conglomerate.

**/

public class B2IStaticCompiledInfo implements StaticCompiledOpenConglomInfo
{

    /**************************************************************************
     * Fields of the class 
     **************************************************************************
     */

    /**
     * Conglomerate data structure for this index.
     **/
    B2I b2i;

    /**
     * Conglomerate data structure for this base table of this index.
     **/
    StaticCompiledOpenConglomInfo   base_table_static_info;

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */

    /**
     * Empty arg constructor used by the monitor to create object to read into.
     **/
    public B2IStaticCompiledInfo()
    {
    }

    /**
     * Constructor used to build class from scratch.
     * <p>
     * @param b2i    the btree Conglomerate that we are compiling.
     **/
    B2IStaticCompiledInfo(
    TransactionController   tc,
    B2I                     b2i) 
        throws StandardException
    {
        this.b2i = b2i;

        this.base_table_static_info = 
            tc.getStaticCompiledConglomInfo(b2i.baseConglomerateId);
    }

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods of StaticCompiledOpenConglomInfo Interface:
     **************************************************************************
     */

    /**
     * return the "Conglomerate"
     * <p>
     * For secondaryindex compiled info return the secondary index conglomerate.
     * <p>
     *
	 * @return the secondary index Conglomerate Object.
     **/
    public DataValueDescriptor getConglom()
    {
        return(b2i);
    }

    /**************************************************************************
     * Public Methods of Storable Interface (via StaticCompiledOpenConglomInfo):
     *     This class is responsible for re/storing its own state.
     **************************************************************************
     */


	/**
	Return whether the value is null or not.
	The containerid being zero is what determines nullness;  subclasses
	are not expected to override this method.
	@see org.apache.derby.iapi.services.io.Storable#isNull
	**/
	public boolean isNull()
	{
		return(b2i == null);
	}

	/**
	Restore the in-memory representation to the null value.
	The containerid being zero is what determines nullness;  subclasses
	are not expected to override this method.

	@see org.apache.derby.iapi.services.io.Storable#restoreToNull
	**/
	public void restoreToNull()
	{
		b2i = null;
	}

    /**
     * Return my format identifier.
     *
     * @see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
     **/
	public int getTypeFormatId()
    {
		return StoredFormatIds.ACCESS_B2I_STATIC_COMPILED_V1_ID;
	}

	/**
	Restore the in-memory representation from the stream.

	@exception ClassNotFoundException Thrown if the stored representation is
	serialized and a class named in the stream could not be found.

    @exception IOException thrown by readObject()

	
	@see java.io.Externalizable#readExternal
	*/
	public void readExternal(ObjectInput in) 
        throws IOException, ClassNotFoundException
	{
        // read in the B2I
        b2i = new B2I();
        b2i.readExternal(in);

        // read in base table conglomerate
        base_table_static_info = 
            (StaticCompiledOpenConglomInfo) in.readObject();
	}
	public void readExternalFromArray(ArrayInputStream in) 
        throws IOException, ClassNotFoundException
	{
        // read in the B2I
        b2i = new B2I();
        b2i.readExternal(in);

        // read in base table conglomerate
        base_table_static_info = 
            (StaticCompiledOpenConglomInfo) in.readObject();
	}
	
	/**
	Store the stored representation of the column value in the stream.
	It might be easier to simply store the properties - which would certainly
	make upgrading easier.

    @exception IOException thrown by writeObject()

	*/
	public void writeExternal(ObjectOutput out) 
        throws IOException
    {
        // first write the B2I object (the type we "know")
        b2i.writeExternal(out);

        // write Conglomerate object as an object
        out.writeObject(base_table_static_info);
	}
}
