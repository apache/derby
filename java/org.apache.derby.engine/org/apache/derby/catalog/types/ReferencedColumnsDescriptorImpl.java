/*

   Derby - Class org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl

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


import org.apache.derby.shared.common.util.ArrayUtil;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.catalog.ReferencedColumns;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.IOException;

/**
 * For triggers, ReferencedColumnsDescriptorImpl object has 3 possibilites
 * 1)referencedColumns is not null but referencedColumnsInTriggerAction
 *   is null - meaning the trigger is defined on specific columns but trigger 
 *   action does not reference any column through old/new transient variables. 
 *   Another reason for referencedColumnsInTriggerAction to be null(even though
 *   trigger action does reference columns though old/new transient variables 
 *   would be that we are in soft-upgrade mode for pre-10.7 databases and 
 *   hence we do not want to write anything about 
 *   referencedColumnsInTriggerAction for backward compatibility (DERBY-1482).
 *   eg create trigger tr1 after update of c1 on t1 for each row values(1); 
 * 2)referencedColumns is null but referencedColumnsInTriggerAction is not null 
 *   - meaning the trigger is not defined on specific columns but trigger 
 *   action references column through old/new transient variables
 *   eg create trigger tr1 after update on t1 referencing old as oldt 
 *      for each row values(oldt.id); 
 * 3)referencedColumns and referencedColumnsInTriggerAction are not null -
 *   meaning the trigger is defined on specific columns and trigger action
 *   references column through old/new transient variables
 *   eg create trigger tr1 after update of c1 on t1 referencing old as oldt 
 *      for each row values(oldt.id); 
 */
public class ReferencedColumnsDescriptorImpl
	implements ReferencedColumns, Formatable
{
	/********************************************************
	**
	**	This class implements Formatable. That means that it
	**	can write itself to and from a formatted stream. If
	**	you add more fields to this class, make sure that you
	**	also write/read them with the writeExternal()/readExternal()
	**	methods.
	**
	**	If, inbetween releases, you add more fields to this class,
	**	then you should bump the version number emitted by the getTypeFormatId()
	**	method.
	**
	********************************************************/

	private int[] referencedColumns;
	private int[] referencedColumnsInTriggerAction;

	/**
	 * Constructor for an ReferencedColumnsDescriptorImpl
	 *
	 * @param referencedColumns The array of referenced columns.
	 */

	public ReferencedColumnsDescriptorImpl(	int[] referencedColumns)
	{
		this.referencedColumns = ArrayUtil.copy( referencedColumns );
	}

	/**
	 * Constructor for an ReferencedColumnsDescriptorImpl
	 *
	 * @param referencedColumns The array of referenced columns.
	 * @param referencedColumnsInTriggerAction The array of referenced columns
	 *   in trigger action through old/new transition variables.
	 */

	public ReferencedColumnsDescriptorImpl(	int[] referencedColumns,
			int[] referencedColumnsInTriggerAction)
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-3177
//IC see: https://issues.apache.org/jira/browse/DERBY-3177
		this.referencedColumns = ArrayUtil.copy( referencedColumns );
		this.referencedColumnsInTriggerAction = ArrayUtil.copy( referencedColumnsInTriggerAction );
	}

	/** Zero-argument constructor for Formatable interface */
	public ReferencedColumnsDescriptorImpl()
	{
	}	
	/**
	* @see ReferencedColumns#getReferencedColumnPositions
	*/
	public int[] getReferencedColumnPositions()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-3177
		return ArrayUtil.copy( referencedColumns );
	}
	
	/**
	* @see ReferencedColumns#getTriggerActionReferencedColumnPositions
	*/
	public int[] getTriggerActionReferencedColumnPositions()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-3177
		return ArrayUtil.copy( referencedColumnsInTriggerAction );
	}

	/* Externalizable interface */

	/**
	 * For triggers, 3 possible scenarios
	 * 1)referencedColumns is not null but referencedColumnsInTriggerAction
	 * is null - then following will get read
	 *   referencedColumns.length
	 *   individual elements from referencedColumns arrary
	 *   eg create trigger tr1 after update of c1 on t1 for each row values(1); 
	 * 2)referencedColumns is null but referencedColumnsInTriggerAction is not 
	 * null - then following will get read
	 *   -1
	 *   -1
	 *   referencedColumnsInTriggerAction.length
	 *   individual elements from referencedColumnsInTriggerAction arrary
	 *   eg create trigger tr1 after update on t1 referencing old as oldt 
	 *      for each row values(oldt.id); 
	 * 3)referencedColumns and referencedColumnsInTriggerAction are not null -
	 *   then following will get read
	 *   -1
	 *   referencedColumns.length
	 *   individual elements from referencedColumns arrary
	 *   referencedColumnsInTriggerAction.length
	 *   individual elements from referencedColumnsInTriggerAction arrary
	 *   eg create trigger tr1 after update of c1 on t1 referencing old as oldt 
	 *      for each row values(oldt.id); 
	 *      
	 *  Scenario 1 for triggers is possible for all different releases of dbs
	 *  ie both pre-10.7 and 10.7(and higher). But scenarios 2 and 3 are only
	 *  possible with database at 10.7 or higher releases. Prior to 10.7, we
	 *  did not collect any trigger action column info and hence
	 *  referencedColumnsInTriggerAction will always be null for triggers
	 *  created prior to 10.7 release. 
	 *      
	 * @see java.io.Externalizable#readExternal
	 *
	 * @exception IOException	Thrown on read error
	 */
	public void readExternal(ObjectInput in) throws IOException 
	{
	        int rcLength; 
	        int versionNumber = in.readInt(); 

        	//A negative value for versionNumber means that the trigger
        	//action has column references through old/new transient
        	//variables. This will never happen with triggers created
        	//prior to 10.7 because prior to 10.7, we did not collect
        	//such information about trigger action columns. 
	        if ( versionNumber < 0 ) {
	        	//Now, check if there any trigger columns identified for
	        	//this trigger.
	            rcLength = in.readInt(); 
	            if ( rcLength < 0 ) { 
	            	//No trigger columns selected for this trigger. This is
	            	//trigger scenario 2(as described in method level comments)
	                rcLength = 0;
	            } else {
	            	//This trigger has both trigger columns and trigger action
	            	//columns. This is trigger scenario 3(as described in
	            	//method level comments)
	                referencedColumns = new int[rcLength];
	            }
	        } else { 
	        	//this trigger only has trigger columns saved on the disc.
	        	//This is trigger scenario 1(as described in method level
	        	//comments)
	            rcLength = versionNumber; 
	            referencedColumns = new int[rcLength]; 
	        } 
	         
	        for (int i = 0; i < rcLength; i++) 
	        { 
	            //if we are in this loop, then it means that this trigger has
	        	//been defined on specific columns.
	            referencedColumns[i] = in.readInt(); 
	        } 

	        if ( versionNumber < 0 ) 
	        { 
	        	//As mentioned earlier, a negative value for versionNumber
	        	//means that this trigger action references columns through
	        	//old/new transient variables.
	            int rctaLength = in.readInt(); 

	            referencedColumnsInTriggerAction = new int[rctaLength];
	            for (int i = 0; i < rctaLength; i++) 
	            { 
	                referencedColumnsInTriggerAction[i] = in.readInt(); 
	            } 
	        } 
	} 

	/**
	 * For triggers, 3 possible scenarios
	 * 1)referencedColumns is not null but referencedColumnsInTriggerAction
	 * is null - then following gets written
	 *   referencedColumns.length
	 *   individual elements from referencedColumns arrary
	 *   
	 * eg create trigger tr1 after update of c1 on t1 for each row values(1); 
	 * This can also happen for a trigger like following if the database is
	 * at pre-10.7 level. This is for backward compatibility reasons because
	 * pre-10.7 releases do not collect/work with trigger action column info
	 * in system table. That functionality has been added starting 10.7 release
	 *   eg create trigger tr1 after update on t1 referencing old as oldt 
	 *      for each row values(oldt.id); 
	 * 2)referencedColumns is null but referencedColumnsInTriggerAction is not 
	 * null - then following gets written
	 *   -1
	 *   -1
	 *   referencedColumnsInTriggerAction.length
	 *   individual elements from referencedColumnsInTriggerAction arrary
	 *   eg create trigger tr1 after update on t1 referencing old as oldt 
	 *      for each row values(oldt.id); 
	 * 3)referencedColumns and referencedColumnsInTriggerAction are not null -
	 *   then following gets written
	 *   -1
	 *   referencedColumns.length
	 *   individual elements from referencedColumns arrary
	 *   referencedColumnsInTriggerAction.length
	 *   individual elements from referencedColumnsInTriggerAction arrary
	 *   eg create trigger tr1 after update of c1 on t1 referencing old as oldt 
	 *      for each row values(oldt.id); 
	 *      
	 * @see java.io.Externalizable#writeExternal
	 *
	 * @exception IOException	Thrown on write error
	 */
	public void writeExternal(ObjectOutput out) throws IOException 
	{ 
		//null value for referencedColumnsInTriggerAction means one of 2 cases
		//1)We are working in soft-upgrade mode dealing with databases lower
		//than 10.7. Prior to 10.7 release, we did not keep track of trigger
		//action columns
		//2)We are working with 10.7(and higher) release database and the
		//trigger action does not reference any column through old/new
		//transient variables

		//versionNumber will be -1 if referencedColumnsInTriggerAction is not
		//null, meaning, we are dealing with 10.7 and higher release database
		//and the trigger has referenced columns in trigger action through
		//old/new transient variables. Otherwise, versionNumber will be the
		//length of the arrary referencedColumns. This arrary holds the columns
		//on which trigger is defined. The detailed meaning of these 2 arrays
		//is described at the class level comments(towards the beginning of
		//this class.
        int versionNumber = referencedColumnsInTriggerAction == null ? referencedColumns.length : -1; 

        if ( versionNumber < 0 ) { 
	        out.writeInt( versionNumber ); 
        	//If we are here, then it means that trigger action references 
        	//columns through old/new transient variables. 
        	//First we will check if there are any trigger columns selected
        	//for this trigger. If yes, we will write information about 
        	//trigger columns and if not, then we will write -1 to indicate 
        	//that there are no trigger columns selected.
        	//After that, we will write info about trigger action columns.
            if ( referencedColumns != null ) { 
            	writeReferencedColumns(out);
            } else {
                out.writeInt(versionNumber);
            }
            //Write info about trigger action columns referenced through 
            //old/new transient variables
            out.writeInt(referencedColumnsInTriggerAction.length); 
            for (int i = 0; i < referencedColumnsInTriggerAction.length; i++) 
            { 
                out.writeInt(referencedColumnsInTriggerAction[i]); 
            } 
        } else {
        	//If we are here, then it means there are no references in 
        	//trigger action to old/new transient variables. But, three are
        	//trigger columns selected for this trigger. Write info about 
        	//trigger columns
        	writeReferencedColumns(out);
        }	         
	} 
	private void writeReferencedColumns(ObjectOutput out) throws IOException 
	{ 
    	//trigger is defined on select columns. Write info about those columns
        out.writeInt( referencedColumns.length ); 
        for (int i = 0; i < referencedColumns.length; i++) 
        { 
            out.writeInt(referencedColumns[i]); 
        } 
	}

	/* TypedFormat interface */
	public int getTypeFormatId()
	{
		return StoredFormatIds.REFERENCED_COLUMNS_DESCRIPTOR_IMPL_V01_ID;
	}

	/**
	  @see java.lang.Object#toString
	  */
	public String	toString()
	{
		if (referencedColumns == null)
			return "NULL";
		
		StringBuffer sb = new StringBuffer(60);

		sb.append('(');
		for (int index = 0; index < referencedColumns.length; index++)
		{
			if (index > 0)
				sb.append(',');
			sb.append(String.valueOf(referencedColumns[index]));

		}
		sb.append(')');
		return sb.toString();
	}
}
