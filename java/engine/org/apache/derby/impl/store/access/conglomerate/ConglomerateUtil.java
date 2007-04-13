/*

   Derby - Class org.apache.derby.impl.store.access.conglomerate.ConglomerateUtil

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

package org.apache.derby.impl.store.access.conglomerate;

import org.apache.derby.iapi.reference.Property;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.CompressedNumber;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.FormatIdUtil;

import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.RowUtil;

import org.apache.derby.iapi.store.raw.FetchDescriptor;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.RecordHandle;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.StringDataValue;

import java.io.IOException; 
import java.io.ObjectInput;
import java.io.ObjectOutput;

import java.util.Properties;

/**
 * Static utility routine package for all Conglomerates.
 * <p>
 * A collection of static utility routines that are shared by multiple
 * Conglomerate implementations.
 * <p>
 **/
public final class ConglomerateUtil
{

    /* Public Methods of This class: (arranged Alphabetically ) */

    /**
     * Create a list of all the properties that Access wants to export
     * through the getInternalTablePropertySet() call.
     * <p>
     * This utility routine creates a list of properties that are shared by
     * all conglomerates.  This list contains the following:
     *
     *     derby.storage.initialPages
     *     derby.storage.minimumRecordSize
     *     derby.storage.pageReservedSpace
     *     derby.storage.pageSize 
	 *     derby.storage.reusableRecordId
     *     
     * <p>
     *
	 * @return The Property set filled in.
     *
     * @param prop   If non-null the property set to fill in.
     **/
    public static Properties createRawStorePropertySet(
    Properties  prop)
    {
        prop = createUserRawStorePropertySet(prop);

        prop.put(RawStoreFactory.PAGE_REUSABLE_RECORD_ID,       "");

        return(prop);
    }

    /**
     * Create a list of all the properties that Access wants to export
     * through the getInternalTablePropertySet() call.
     * <p>
     * This utility routine creates a list of properties that are shared by
     * all conglomerates.  This list contains the following:
     *
     *     derby.storage.initialPages
     *     derby.storage.minimumRecordSize
     *     derby.storage.pageReservedSpace
     *     derby.storage.pageSize 
     *     
     * <p>
     *
	 * @return The Property set filled in.
     *
     * @param prop   If non-null the property set to fill in.
     **/
    public static Properties createUserRawStorePropertySet(
    Properties  prop)
    {
        if (prop == null)
            prop = new Properties();

        prop.put(Property.PAGE_SIZE_PARAMETER,           "");
        prop.put(RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER, "");
        prop.put(RawStoreFactory.PAGE_RESERVED_SPACE_PARAMETER, "");
        prop.put(RawStoreFactory.CONTAINER_INITIAL_PAGES,       "");

        return(prop);
    }


    /**
     * Given an array of objects, return an array of format id's.
     * <p>
     *
	 * @return An array of format id's describing the input array of objects.
     *
     * @param template a row.
     *
     **/
    public static int[] createFormatIds(
    DataValueDescriptor[]    template)
    {

        // get format id's from each column in template
        // conglomerate state.

        int[] format_ids = new int[template.length];

        for (int i = 0; i < template.length; i++)
        {
            if (SanityManager.DEBUG)
            {
				if (template[i] == null)
				{
                	SanityManager.THROWASSERT("row template is null for "+
							"column["+i+"].");
				}
				if (!(template[i] instanceof Formatable))
				{
                	SanityManager.THROWASSERT("row template is not formatable "+
							"column["+i+"].  Type is "+template[i].getClass().getName());
				}
            }

            format_ids[i] = ((Formatable) template[i]).getTypeFormatId();
        }

        return(format_ids);
    }

    /**
     * Read a format id array in from a stream.
     * <p>
     *
	 * @return A new array of format id's.
     *
     * @param num         The number of format ids to read.
     * @param in          The stream to read the array of format id's from.
     *
	 * @exception  IOException  Thown on read error.
     **/
    public static int[] readFormatIdArray(
    int         num,
    ObjectInput in)
        throws IOException
    {
        // read in the array of format id's

        int[] format_ids = new int[num];
        for (int i = 0; i < num; i++)
        {
            format_ids[i] = FormatIdUtil.readFormatIdInteger(in);
        }

        return(format_ids);
    }

    /**
     * Write a format id array to a stream.
     * <p>
     *
     * @param format_id_array The array of format ids to write.
     * @param out             The stream to write the array of format id's to.
     *
	 * @exception  IOException  Thown on write error.
     **/
    public static void writeFormatIdArray(
    int[]     format_id_array,
    ObjectOutput out)
        throws IOException
    {
        for (int i = 0; i < format_id_array.length; i++)
        {
            FormatIdUtil.writeFormatIdInteger(out, format_id_array[i]);
        }
    }

    /**
     * Given an array of columnOrderings, return an array of collation ids.
     * <p>
     * If input array is null, produce a default collation_id array of all
     * StringDataValue.COLLATION_TYPE_UCS_BASIC values.
     *
     * @return An array of collation id's describing the input array of objects.
     **/
    public static int[] createCollationIds(
    int     sizeof_ids,
    int[]   collationIds)
    {
        int[] collation_ids = new int[sizeof_ids];
        if (collationIds != null)
        {
            if (SanityManager.DEBUG)
            {
                if (sizeof_ids != collationIds.length)
                {
                    SanityManager.THROWASSERT(
                        "sizeof_ids = " + sizeof_ids +
                        ";collationIds.length = " + collationIds.length);
                }
            }
            System.arraycopy(
                collationIds, 0, collation_ids, 0, collationIds.length);
        }
        else
        {
            for (int i = 0; i < collation_ids.length; i++)
            {
                collation_ids[i] = StringDataValue.COLLATION_TYPE_UCS_BASIC;
            }
        }

        return(collation_ids);
    }

    /**
     * Write array of collation id's as a sparse array.
     * <p>
     * The format only writes out those array entries which are not 
     * StringDataValue.COLLATION_TYPE_UCS_BASIC.  The sparse array
     * first writes the number of entries as a compressed int.  And
     * then for each non-COLLATION_TYPE_UCS_BASIC, it writes out a
     * pair of compressed ints:
     *
     *     (array offset, array entry value)
     *
     * @param collation_id_array The array of collation ids to write.
     * @param out                The stream to write the collation id's to.
     *
	 * @exception  IOException  Thown on write error.
     **/
    public static void writeCollationIdArray(
    int[]           collation_id_array, 
    ObjectOutput    out)
        throws IOException
    {
        // count non COLLATION_TYPE_UCS_BASIC values.
        int non_collate_val_count = 0;
        for (int i = 0; i < collation_id_array.length; i++)
        {
            if (collation_id_array[i] != 
                    StringDataValue.COLLATION_TYPE_UCS_BASIC)
            {
                non_collate_val_count++;
            }
        }

        // write number of sparse entries as compressed int
        CompressedNumber.writeInt(out, non_collate_val_count);

        for (int i = 0; i < collation_id_array.length; i++)
        {
            if (collation_id_array[i] != 
                    StringDataValue.COLLATION_TYPE_UCS_BASIC)
            {
                // write array index as compressed number
                CompressedNumber.writeInt(out, i);

                // write array[i] value as compressed number
                CompressedNumber.writeInt(out, collation_id_array[i]);
            }
        }
    }

    /**
     * Read "sparse" array of collation id's
     * <p>
     * The format to be read first has the number of entries as a compressed 
     * int.  And then for each non-COLLATION_TYPE_UCS_BASIC value there is
     * pair of compressed ints:
     *
     *     (array offset, array entry value)
     * <p>
     * reads the sparse array as written by writeCollationIdArray().
     *
     * @param collation_id_array update's only those array entries that have
     *                           been set in the sparse array stream.
     *                           Those values are set as indicated by reading 
     *                           the sparse array from the stream.
     *                           
     * @param in                 The stream to read the collation info from.
     *
     **/
    public static void readCollationIdArray(
    int[]           collation_id_array,
    ObjectInput     in)
        throws IOException
	{

        // A sparse array is stored on disk, only 
        // non-COLLATION_TYPE_UCS_BASIC values are stored.  
        // These are stored as pairs of compressed ints:
        //     (array offset, array entry value)


        // 1st on disk is number of entries stored as compressed a int
        int num_compressed_entries = CompressedNumber.readInt(in);
        for (int i = 0; i < num_compressed_entries; i++)
        {
            // values are stored in the stream as pairs: (index, value)
            int array_index = CompressedNumber.readInt(in);
            collation_id_array[array_index] = CompressedNumber.readInt(in);
        }
	}

	/**
	 ** Format a page of data, as access see's it.
	 **/

	public static String debugPage(
    Page                    page,
    int                     start_slot,
    boolean                 full_rh,
    DataValueDescriptor[]   template)
    {
        if (SanityManager.DEBUG)
        {
            StringBuffer string = new StringBuffer(4096);

            string.append("PAGE:(");
            string.append(page.getPageNumber());
            string.append(")------------------------------------------:\n");

            try
            {
                if (page != null)
                {
                    int numrows   = page.recordCount();

                    for (int slot_no = start_slot; slot_no < numrows; slot_no++)
                    {
                        RecordHandle rh = 
                            page.fetchFromSlot(
                               (RecordHandle) null, slot_no, template, 
                               (FetchDescriptor) null,
                               true);

                        // pre-pend either "D:" if deleted, or " :" if not.
                        string.append(
                            page.isDeletedAtSlot(slot_no) ? "D:" : " :");

                        // row[slot,id]:
                        string.append("row[");
                        string.append(slot_no);
                        string.append("](id:");
                        string.append(rh.getId());
                        string.append("):\t");

                        // long record handle: 
                        //   Record id=78 Page(31,Container(0, 919707766934))
                        if (full_rh)
                        {
                            string.append("[");
                            string.append(rh.toString());
                            string.append("]:");
                        }

                        // row:
                        string.append(RowUtil.toString(template));
                        string.append("\n");
                    }

                    // string.append(page.toString());
                }
            }
            catch (Throwable t)
            {
                string.append("Error encountered while building string");
            }

            return(string.toString());
        }
        else
        {
            return(null);
        }
    }
}
