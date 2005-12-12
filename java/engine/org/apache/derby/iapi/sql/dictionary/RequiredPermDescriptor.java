/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.RequiredPermDescriptor

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

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.services.io.FormatableBitSet;

/**
 * This class implements a row in the SYS.SYSREQUIREDPERM table, which keeps
 * track of the permissions required by views, triggers, and constraints.
 *
 */
public class RequiredPermDescriptor extends TupleDescriptor
{
	private UUID operatorUUID;
	private String operatorType;
	private String permType;
	private UUID objectUUID;
	private FormatableBitSet columns;
	
	public RequiredPermDescriptor( UUID operatorUUID,
								   String operatorType,
								   String permType,
								   UUID objectUUID,
								   FormatableBitSet columns)
	{
		this.operatorUUID = operatorUUID;
		this.operatorType = operatorType;
		this.permType = permType;
		this.objectUUID = objectUUID;
		this.columns = columns;
	}
	
	/*----- getter functions for rowfactory ------*/
	public UUID getOperatorUUID() { return operatorUUID;}
	public String getOperatorType() { return operatorType;}
	public String getPermType() { return permType;}
	public UUID getObjectUUID() { return objectUUID;}
	public FormatableBitSet getColumns() { return columns;}

	public String toString()
	{
		return "RequiredPerm: operatorUUID=" + getOperatorUUID() + 
			",operatortype=" + getOperatorType() +
		  ",permtype=" + getPermType() +
		  ",objectUUID=" + getObjectUUID() +
		  ",columns=" + getColumns();
	}		
}
