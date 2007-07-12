/*

   Derby - Class org.apache.derby.impl.store.access.btree.index.B2I

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.store.access.heap;


import org.apache.derby.iapi.services.io.StoredFormatIds; 

import java.io.IOException; 
import java.io.ObjectOutput;
import java.io.ObjectInput;

import java.lang.ClassNotFoundException;


/**
 * @format_id ACCESS_HEAP_V2_ID
 *
 * @purpose   The tag that describes the on disk representation of the Heap
 *            conglomerate object.  Access contains no "directory" of 
 *            conglomerate information.  In order to bootstrap opening a file
 *            it encodes the factory that can open the conglomerate in the 
 *            conglomerate id itself.  There exists a single HeapFactory which
 *            must be able to read all heap format id's.  
 *
 *            This format was used for all Derby database Heap's in version
 *            10.2 and previous versions.
 *
 * @upgrade   The format id of this object is currently always read from disk
 *            as the first field of the conglomerate itself.  A bootstrap
 *            problem exists as we don't know the format id of the heap 
 *            until we are in the "middle" of reading the Heap.  Thus the
 *            base Heap implementation must be able to read and write 
 *            all formats based on the reading the 
 *            "format_of_this_conglomerate". 
 *
 *            soft upgrade to ACCESS_HEAP_V3_ID:
 *                read:
 *                    old format is readable by current Heap implementation,
 *                    with automatic in memory creation of default collation
 *                    id needed by new format.  No code other than
 *                    readExternal and writeExternal need know about old format.
 *                write:
 *                    will never write out new format id in soft upgrade mode.
 *                    Code in readExternal and writeExternal handles writing
 *                    correct version.  Code in the factory handles making
 *                    sure new conglomerates use the Heap_v10_2 class
 *                    that will write out old format info.
 *
 *            hard upgrade to ACCESS_HEAP_V3_ID:
 *                read:
 *                    old format is readable by current Heap implementation,
 *                    with automatic in memory creation of default collation
 *                    id needed by new format.
 *                write:
 *                    Only "lazy" upgrade will happen.  New format will only
 *                    get written for new conglomerate created after the 
 *                    upgrade.  Old conglomerates continue to be handled the
 *                    same as soft upgrade.
 *
 * @disk_layout
 *     format_of_this_conlgomerate(byte[])
 *     containerid(long)
 *     segmentid(int)
 *     number_of_columns(int)
 *     array_of_format_ids(byte[][])
 **/



public class Heap_v10_2 extends Heap
{

    /**
     * No arg constructor, required by Formatable.
     **/
    public Heap_v10_2()
    {
        super();
    }


    /**************************************************************************
     * Public Methods required by Storable interface, implies 
     *     Externalizable, TypedFormat:
     **************************************************************************
     */

    /**
     * Return my format identifier.
     * <p>
     * This identifier was used for Heap in all Derby versions prior to 10.3.
     * Databases hard upgraded to a version 10.3 and later will write the new 
     * format, see Heap.  Databases created in 10.3 and later will also write 
     * the new format, see Heap.
     *
     * @see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
     **/
	public int getTypeFormatId() 
    {
        // return identifier used for Heap in all derby versions prior to 10.3
		return StoredFormatIds.ACCESS_HEAP_V2_ID;
	}

    /**
     * Store the stored representation of the column value in the
     * stream.
     * <p>
     * For more detailed description of the format see documentation
     * at top of file.
     *
     * @see java.io.Externalizable#writeExternal
     **/
	public void writeExternal(ObjectOutput out) throws IOException 
    {
		super.writeExternal_v10_2(out);
	}
}
