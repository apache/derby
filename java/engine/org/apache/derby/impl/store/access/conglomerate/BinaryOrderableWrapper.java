/*

   Derby - Class org.apache.derby.impl.store.access.conglomerate.BinaryOrderableWrapper

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

package org.apache.derby.impl.store.access.conglomerate;


import org.apache.derby.iapi.services.io.ArrayInputStream;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.iapi.services.io.TypedFormat;

import org.apache.derby.iapi.error.StandardException; 

import org.apache.derby.iapi.store.access.BinaryOrderable;

import java.io.Externalizable;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**

The BinaryOrderableWrapper is a wrapper class which intercepts the 
readExternal() callback made by raw store during a fetch, and does a comparison
instead.
**/

class BinaryOrderableWrapper implements Storable
{

    BinaryOrderable    ref_object; 
    BinaryOrderable    other_object;
    int                cmp_result;

    /* Constructors for This class: */
    BinaryOrderableWrapper()
    {
    }

    /* Private/Protected methods of This class: */
    /**
     * Short one line description of routine.
     * <p>
     * Longer descrption of routine.
     * <p>
     *
	 * @return The identifier to be used to open the conglomerate later.
     *
     * @param ref_object    The object that this object is wrapping (ie. being
     *                      read from disk)
     * @param other_object  The object to compare ref_object to.
     **/
    protected void init (
    BinaryOrderable ref_object,
    BinaryOrderable other_object)
    {
        this.ref_object     = ref_object;
        this.other_object   = other_object;
    }

    /* Public Methods of This class: */
    /**
     * Short one line description of routine.
     * <p>
     * Longer descrption of routine.
     * <p>
     *
	 * @return The identifier to be used to open the conglomerate later.
     *
     * @param param1 param1 does this.
     * @param param2 param2 does this.
     **/
    public int getCmpResult()
    {
        return(this.cmp_result);
    }


    /* Public Methods of Storable interface - Externalizable, TypedFormat: 
    */

	public int getTypeFormatId() {
        // RESOLVE - what should this return?
        if (SanityManager.DEBUG)
            SanityManager.THROWASSERT("If someone calls this it is a problem.");
        return(((TypedFormat)this.ref_object).getTypeFormatId());
	}

	/**
	Return whether the value is null or not.
	The containerid being zero is what determines nullness;  subclasses
	are not expected to override this method.
	@see org.apache.derby.iapi.services.io.Storable#isNull
	**/
	public boolean isNull()
	{
        // RESOLVE - what does it mean for this wrapper to be called isNull()?
        if (SanityManager.DEBUG)
            SanityManager.THROWASSERT("If someone calls this it is a problem.");
        return(false);
	}

	/**
	Restore the in-memory representation to the null value.
	The containerid being zero is what determines nullness;  subclasses
	are not expected to override this method.

	@see org.apache.derby.iapi.services.io.Storable#restoreToNull
	**/
	public void restoreToNull()
	{
        // RESOLVE - base object is null.
        if (SanityManager.DEBUG)
            SanityManager.THROWASSERT("WORK TODO - code up null compare.");

        return;
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

        // do the read byte by byte and return the comparison 
        this.cmp_result = this.ref_object.binarycompare(in, this.other_object);
        
        if (SanityManager.DEBUG)
            SanityManager.THROWASSERT("WORK TODO - code up readExternal.");
	}
	public void readExternalFromArray(ArrayInputStream in) 
        throws IOException, ClassNotFoundException
	{

        // do the read byte by byte and return the comparison 
        this.cmp_result = this.ref_object.binarycompare(in, this.other_object);
        
        if (SanityManager.DEBUG)
            SanityManager.THROWASSERT("WORK TODO - code up readExternal.");
	}
	
    /**
     * Store the stored representation of the column value in the stream.
     * <p>
     * A BinaryOrderableWrapper is never used to store data out, only to read
     * data from disk and compare it to another byte stream.
     *
     * @param out    Stream to write the object to.
     *
     * @exception IOException thrown by writeObject()
     *
     **/
	public void writeExternal(ObjectOutput out) 
        throws IOException
    {
        if (SanityManager.DEBUG)
            SanityManager.THROWASSERT("Write should never be called.");
        return;
	}
}
