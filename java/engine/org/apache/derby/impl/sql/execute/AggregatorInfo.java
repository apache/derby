/*

   Derby - Class org.apache.derby.impl.sql.execute.AggregatorInfo

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.sanity.SanityManager;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
/**
 * This is a simple class used to store the run time information
 * needed to invoke an aggregator.  This class is serializable
 * because it is stored with the plan.  It is serializable rather
 * than externalizable because it isn't particularly complicated
 * and presumbably we don't need version control on plans.
 *
 * @author jamie
 */
public class AggregatorInfo implements Formatable 
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
	**	method.  OR, since this is something that is used
	**	in stored prepared statements, it is ok to change it
	**	if you make sure that stored prepared statements are
	**	invalidated across releases.
	**
	********************************************************/

	/*
	** See the constructor for the meaning of these fields
	*/
	String	aggregateName;
	int		inputColumn;
	int		outputColumn;
	int		aggregatorColumn;
	String	aggregatorClassName;
	boolean	isDistinct;
	ResultDescription	rd;

	/**
	 * Niladic constructor for Formattable
	 */
	public AggregatorInfo() {}

	/**
	 * Consructor
	 *
	 * @param aggregateName	the name of the aggregate.  Not
 	 *		actually used anywhere except diagnostics.  Should
	 *		be the names as found in the language (e.g. MAX).
	 * @param aggregatorClassName	the name of the aggregator
	 *		used to process this aggregate.  Aggregator expected
	 *		to have a null arg constructor and implement
	 *		Aggregator.
	 * @param inputColNum	the input column number
	 * @param outputColNum	the output column number
	 * @param aggregatorColNum	the column number in which the 
	 *		aggregator is stored.
	 * @param isDistinct	if it is a distinct aggregate
	 * @param rd	the result description
	 *
	 */
	public AggregatorInfo
	(
		String 				aggregateName, 
		String				aggregatorClassName,
		int					inputColNum,
		int					outputColNum,
		int					aggregatorColNum,
		boolean				isDistinct,
		ResultDescription	rd
	)
	{
		this.aggregateName	= aggregateName;
		this.aggregatorClassName = aggregatorClassName;
		this.inputColumn	= inputColNum;	
		this.outputColumn	= outputColNum;
		this.aggregatorColumn = aggregatorColNum;
		this.isDistinct 	= isDistinct;
		this.rd 			= rd;
	}

	/**
	 * Get the name of the aggergate (e.g. MAX)
	 *
	 * @return the aggeregate name
	 */
	public String getAggregateName()
	{
		return aggregateName;
	}

	/**
	 * Get the name of the class that implements the user
	 * aggregator for this class.
	 *
	 * @return the aggeregator class name
	 */
	public String getAggregatorClassName()
	{
		return aggregatorClassName;
	}


	/**
	 * Get the column number for the aggregator
	 * column.
	 *
	 * @return the aggeregator colid
	 */
	public int getAggregatorColNum()
	{
		return aggregatorColumn;
	}

	/**
	 * Get the column number for the input
	 * (addend) column.
	 *
	 * @return the aggeregator colid
	 */
	public int getInputColNum()
	{
		return inputColumn;
	}

	/**
	 * Get the column number for the output
	 * (result) column.
	 *
	 * @return the aggeregator colid
	 */
	public int getOutputColNum()
	{
		return outputColumn;
	}

	/**
	 * Is the aggergate distinct
	 *
	 * @return whether it is distinct
	 */
	public boolean isDistinct()
	{
		return isDistinct;
	}

	/**
	 * Get the result description for the input value
	 * to this aggregate.
	 *
	 * @return the rd
	 */
	public ResultDescription getResultDescription()
	{
		return rd;
	}

	/**
	 * Get a string for the object
	 *
	 * @return string
	 */
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "AggregatorInfo = Name: "+ aggregateName +
				"\n\tClass: " + aggregatorClassName +
				"\n\tInputColNum: " + inputColumn +
				"\n\tOutputColNum: " + outputColumn +
				"\n\tAggregatorColNum: " + aggregatorColumn +
				"\n\tDistinct: " + isDistinct +
				"\n" + rd;
		}
		else
		{
			return "";
		}
	}


	//////////////////////////////////////////////
	//
	// FORMATABLE
	//
	//////////////////////////////////////////////
	/**
	 * Write this object out
	 *
	 * @param out write bytes here
	 *
 	 * @exception IOException thrown on error
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeObject(aggregateName);
		out.writeInt(inputColumn);
		out.writeInt(outputColumn);
		out.writeInt(aggregatorColumn);
		out.writeObject(aggregatorClassName);
		out.writeBoolean(isDistinct);
		out.writeObject(rd);
	}

	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
	public void readExternal(ObjectInput in)
		throws IOException, ClassNotFoundException
	{
		aggregateName = (String)in.readObject();
		inputColumn = in.readInt();
		outputColumn = in.readInt();
		aggregatorColumn = in.readInt();
		aggregatorClassName = (String)in.readObject();
		isDistinct = in.readBoolean();
		rd = (ResultDescription)in.readObject();
	}
	
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public int	getTypeFormatId()	{ return StoredFormatIds.AGG_INFO_V01_ID; }
}
