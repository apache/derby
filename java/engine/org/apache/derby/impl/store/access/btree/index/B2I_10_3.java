/*

   Derby - Class org.apache.derby.impl.store.access.btree.index.B2I_10_3

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

package org.apache.derby.impl.store.access.btree.index;

import java.io.IOException;
import java.io.ObjectOutput;
import org.apache.derby.iapi.services.io.StoredFormatIds;

/*
 * @format_id ACCESS_B2I_V4_ID
 *
 * @purpose   The tag that describes the on disk representation of the B2I
 *            conglomerate object.  Access contains no "directory" of 
 *            conglomerate information.  In order to bootstrap opening a file
 *            it encodes the factory that can open the conglomerate in the 
 *            conglomerate id itself.  There exists a single B2IFactory which
 *            must be able to read all btree format id's.  
 *
 *            This format was used for all Derby database B2I's in version 10.3.
 *
 * @upgrade   The format id of this object is currently always read from disk
 *            as the first field of the conglomerate itself.  A bootstrap
 *            problem exists as we don't know the format id of the B2I 
 *            until we are in the "middle" of reading the B2I.  Thus the
 *            base B2I implementation must be able to read and write 
 *            all formats based on the reading the 
 *            "format_of_this_conglomerate". 
 *
 *            soft upgrade to ACCESS_B2I_V5_ID:
 *                read:
 *                    old format is readable by current B2I implementation,
 *                    with automatic in memory creation of default
 *                    isUniqueWithDuplicateNulls value of false.
 *                    No code other than readExternal and writeExternal need 
 *                    know about old format.
 *                write:
 *                    will never write out new format id in soft upgrade mode.
 *                    Code in readExternal and writeExternal handles writing
 *                    correct version.  Code in the factory handles making
 *                    sure new conglomerates use the B2I_v10_3 class
 *                    that will write out old format info.
 *
 *            hard upgrade to ACCESS_B2I_V5_ID:
 *                read:
 *                    old format is readable by current B2I implementation,
 *                    with automatic in memory creation of default
 *                    isUniqueWithDuplicateNulls value of false.
 *
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
 *     number_of_key_fields(int)
 *     number_of_unique_columns(int)
 *     allow_duplicates(boolean)
 *     maintain_parent_links(boolean)
 *     array_of_format_ids(byte[][])
 *     baseConglomerateId(long)
 *     rowLocationColumn(int)
 *     ascend_column_info(FormatableBitSet)
 *     collation_ids(compressed array of ints)
 *
 */

/**
 * Class used to instantiate 10.3 version of the B2I object.
 *
 * This class implements the format of the B2I object as existed in 
 * the 10.3 release of Derby.  In subsequent releases
 * the format was enhanced to store the uniqueWithDuplicateNulls attribute
 * of the index.
 *
 * For upgrade purpose all 10.3 and prior versions are assumed to have
 * false for the uniqueWithDuplicateNulls attribute.
 *
 * This class reads and writes the V4 version to/from disk and reads/writes
 * current in-memory version of the data structure.
 *
 */
public class B2I_10_3 extends B2I {

    /**
     * Return my format identifier.
     * @see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
     */
    public int getTypeFormatId() {
        return StoredFormatIds.ACCESS_B2I_V4_ID;
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
    
    public void writeExternal(ObjectOutput out) throws IOException {
        writeExternal_v10_3(out);
    }
    
}
