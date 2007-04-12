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
 *            conglomerate object.  The Heap conglomerate object is stored in
 *            a field of a row in the Conglomerate directory.
 *
 * @upgrade   The format id of this object is currently always read from disk
 *            as a separate column in the conglomerate directory.  To read
 *            A conglomerate object from disk and upgrade it to the current
 *            version do the following:
 *
 *                format_id = get format id from a separate column
 *                Upgradable conglom_obj = instantiate empty obj(format_id)
 *                read in conglom_obj from disk
 *                conglom = conglom_obj.upgradeToCurrent();
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
     * This identifier was used for Heap in all Derby versions prior to and
     * including 10.2.  Databases hard upgraded to a version subsequent
     * to 10.2 will write the new format, see Heap.  Databases created in
     * a version subsequent to 10.2 will also write the new formate, see
     * Heap.
     *
     * @see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
     **/
	public int getTypeFormatId() 
    {
		return StoredFormatIds.ACCESS_HEAP_V3_ID;
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
