/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.DDUtils

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

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.StatementType;
import java.util.Hashtable;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.i18n.MessageService;
import java.util.Enumeration;

/**
 *	Static Data dictionary utilities.
 *
 * @version 0.1
 * @author Rick Hillegas
 */

public	class	DDUtils
{

	/*
	** For a foreign key, this is used to locate the referenced
	** key using the ConstraintInfo.  If it doesn't find the
	** correct constraint it will throw an error.
	*/
	public	static ReferencedKeyConstraintDescriptor locateReferencedConstraint
	(
		DataDictionary	dd,
		TableDescriptor	td,
		String			myConstraintName,	// for error messages
		String[]		myColumnNames,
		ConsInfo		otherConstraintInfo
	)
		throws StandardException
	{
		TableDescriptor refTd = otherConstraintInfo.getReferencedTableDescriptor(dd);
		if (refTd == null)
		{
				throw StandardException.newException(SQLState.LANG_INVALID_FK_NO_REF_TAB, 
												myConstraintName, 
												otherConstraintInfo.getReferencedTableName());
		}


		ReferencedKeyConstraintDescriptor refCd = null;

		/*
		** There were no column names specified, just find
		** the primary key on the table in question
		*/
		String[]	refColumnNames = otherConstraintInfo.getReferencedColumnNames();
		if (refColumnNames == null ||
			refColumnNames.length == 0)
		{
			refCd = refTd.getPrimaryKey();
			if (refCd == null)
			{
				throw StandardException.newException(SQLState.LANG_INVALID_FK_NO_PK, 
												myConstraintName, 
												refTd.getQualifiedName());
			}

			ColumnDescriptorList cdl = getColumnDescriptors(dd, td, myColumnNames);

			/*
			** Check the column list length to give a more informative
			** error in case they aren't the same.
			*/
			if (cdl.size() != refCd.getColumnDescriptors().size())
			{
				throw StandardException.newException(SQLState.LANG_INVALID_FK_DIFFERENT_COL_COUNT, 
												myConstraintName, String.valueOf(cdl.size()), 
												String.valueOf(refCd.getColumnDescriptors().size())); 
			}
	
			/*
			** Make sure all types are the same.
			*/	
			if (!refCd.areColumnsComparable(cdl))
			{
				throw StandardException.newException(SQLState.LANG_INVALID_FK_COL_TYPES_DO_NOT_MATCH, 
												myConstraintName);
			}

			return refCd;	
		}

		/*
		** Check the referenced columns vs. each unique or primary key to
		** see if they match the foreign key.
		*/
		else
		{
			ConstraintDescriptor cd;

			ColumnDescriptorList colDl = getColumnDescriptors(dd, td, myColumnNames);
			ConstraintDescriptorList refCDL = dd.getConstraintDescriptors(refTd);

			int refCDLSize = refCDL.size();
			for (int index = 0; index < refCDLSize; index++)
			{
				cd = refCDL.elementAt(index);

				/*
				** Matches if it is not a check or fk, and
				** all the types line up.
				*/
				if ((cd instanceof ReferencedKeyConstraintDescriptor) &&
					 cd.areColumnsComparable(colDl) &&
					 columnNamesMatch(refColumnNames, 
										cd.getColumnDescriptors()))
				{
					return (ReferencedKeyConstraintDescriptor)cd;
				}
			}

			/*
			** If we got here, we didn't find anything
			*/
			throw StandardException.newException(SQLState.LANG_INVALID_FK_NO_REF_KEY, myConstraintName, 
														refTd.getQualifiedName());
		}
	}

    public	static ColumnDescriptorList getColumnDescriptors
	(
		DataDictionary	dd,
		TableDescriptor td,
		String[] 		columnNames
	)
		throws StandardException
	{
		ColumnDescriptorList cdl = new ColumnDescriptorList();
		for (int colCtr = 0; colCtr < columnNames.length; colCtr++)
		{
			ColumnDescriptor cd = td.getColumnDescriptor(columnNames[colCtr]);
			cdl.add(td.getUUID(), cd);
		}
		return cdl;
	}

	public	static boolean columnNamesMatch(String []columnNames, ColumnDescriptorList cdl)
		throws StandardException
	{
		if (columnNames.length != cdl.size())
		{
			return false;
		}
		
		String name;
		for (int index = 0; index < columnNames.length; index++)
		{
			name = ((ColumnDescriptor) cdl.elementAt(index)).getColumnName();
			if (!name.equals(columnNames[index]))
			{
				return false;
			}
		}

		return true;
	}


	/*
	**checks whether the foreign key relation ships referential action
	**is violating the restrictions we have in the current system.
	**/
	public static void validateReferentialActions
    (
		DataDictionary	dd,
		TableDescriptor	td,
		String			myConstraintName,	// for error messages
		ConsInfo		otherConstraintInfo,
		String[]        columnNames
	)
		throws StandardException
	{


		int refAction = otherConstraintInfo.getReferentialActionDeleteRule();

		//Do not allow ON DELETE SET NULL as a referential action 
		//if none of the foreign key columns are  nullable.
		if(refAction == StatementType.RA_SETNULL)
		{
			boolean foundNullableColumn = false;
			//check if we have a nullable foreign key column
			for (int colCtr = 0; colCtr < columnNames.length; colCtr++)
			{
				ColumnDescriptor cd = td.getColumnDescriptor(columnNames[colCtr]);
				if ((cd.getType().isNullable()))
				{
					foundNullableColumn = true;
					break;
				}
			}

			if(!foundNullableColumn)
			{
				throw StandardException.newException(SQLState.LANG_INVALID_FK_COL_FOR_SETNULL, 
													 myConstraintName);
			}
		}

		//check whether the foreign key relation ships referential action
		//is not violating the restrictions we have in the current system.
		TableDescriptor refTd = otherConstraintInfo.getReferencedTableDescriptor(dd);
		Hashtable deleteConnHashtable = new Hashtable();
		//find whether the foreign key is self referencing.
		boolean isSelfReferencingFk = (refTd.getUUID().equals(td.getUUID()));
		String refTableName = refTd.getSchemaName() + "." + refTd.getName();
		//look for the other foreign key constraints on this table first
		int currentSelfRefValue = getCurrentDeleteConnections(dd, td, -1, deleteConnHashtable, false, true);
		validateDeleteConnection(dd, td, refTd, 
								 refAction, 
								 deleteConnHashtable, (Hashtable) deleteConnHashtable.clone(),
								 true, myConstraintName, false , 
								 new StringBuffer(0), refTableName,
								 isSelfReferencingFk,
								 currentSelfRefValue);

		//if it not a selfreferencing key check for violation of exiting connections.
		if(!isSelfReferencingFk)
		{
			checkForAnyExistingDeleteConnectionViolations(dd, td,
														  refAction, 
														  deleteConnHashtable, 
														  myConstraintName);
		}	
	}

	/*
	** Finds the existing delete connection for the table and the referential
	** actions that will occur  and stores the information in the hash table.
	** HashTable (key , value) = ( table name that this table is delete
	** connected to, referential action that will occur if there is a delete on
	** the table this table connected to[CASACDE, SETNULL , RESTRICT ...etc).)
	**/

	private	static int  getCurrentDeleteConnections
	(
	 DataDictionary	dd,
	 TableDescriptor	td,
	 int refActionType,
	 Hashtable dch,
	 boolean prevNotCascade,
	 boolean findSelfRef
	 )
		throws StandardException
	{

		int selfRefValue = -1; //store the self reference referential action 

		//make sure we get any foreign key constraints added earlier in the same statement.
		td.emptyConstraintDescriptorList();
		ConstraintDescriptorList cdl = dd.getConstraintDescriptors(td);
		int cdlSize = cdl.size();

		boolean passedInPrevNotCascade = prevNotCascade;
		for (int index = 0; index < cdlSize; index++)
		{
				ConstraintDescriptor cd = cdl.elementAt(index);

				//look for  foreign keys
				if ((cd instanceof ForeignKeyConstraintDescriptor))
				{
					ForeignKeyConstraintDescriptor fkcd = (ForeignKeyConstraintDescriptor) cd;
					String constraintName = fkcd.getConstraintName();
					int raDeleteRule = fkcd.getRaDeleteRule();
					int raUpdateRule = fkcd.getRaUpdateRule();

					 if(findSelfRef && fkcd.isSelfReferencingFK())
					 {
						 //All self references will have same  referential actions type
						 selfRefValue = raDeleteRule;
						 findSelfRef = false;
					 }

					ReferencedKeyConstraintDescriptor refcd =
						fkcd.getReferencedConstraint(); 
					TableDescriptor refTd = refcd.getTableDescriptor();
					int childRefAction = refActionType == -1 ? raDeleteRule : refActionType;
				   
					String refTableName = refTd.getSchemaName() + "." + refTd.getName();
					//check with  the existing references.
					Integer rAction = ((Integer)dch.get(refTableName));
					if(rAction != null) // we already looked at this table
					{
						prevNotCascade = passedInPrevNotCascade;
						continue;
					}

					//if we are not cascading, check whether the link before
					//this was cascade or not. If we travel through  two NON CASCADE ACTION
					//links then the  delete connection is broken(only a delete can have further
					// referential effects)
					if(raDeleteRule != StatementType.RA_CASCADE)
					{
						if(prevNotCascade)
						{
							prevNotCascade = passedInPrevNotCascade;
							continue;
						}
						else
							prevNotCascade = true;
					}

					//store the delete connection info in the hash table,
					//note that the referential action value is not what is
					//not specified on the current link. It is actually the 
					//value of what happens to the table whose delete
					// connections we are finding.
					dch.put(refTableName, (new Integer(childRefAction)));
					
					//find the next delete conectiions on this path for non
					//self referencig delete connections.
					if(!fkcd.isSelfReferencingFK())
						getCurrentDeleteConnections(dd , refTd, childRefAction,
													dch, true, false);
					prevNotCascade = passedInPrevNotCascade;
				}
		}
		
		return selfRefValue;
	}


	/*
	** Following function validates whether the new foreign key relation ship
	** violates any restriction on the referential actions. Current refAction
	** implementation does not allow cases where we can possible land up
	** having multiple action for the same row in a table, this happens becase
	** user can possibly define differential action through multiple paths.
	** Following function throws error while creating foreign keys if the new
	** releations ship leads to any such conditions.
	** NOTE : SQL99 standard also does not cleary says what we are suppose to do
	** in these non determenistic cases. 
	** Our implementation just follows what is did in DB2 and throws error
	** messaged similar to DB2 (sql0632N, sql0633N, sql0634N)
	*/

	private	static void validateDeleteConnection
	(
		DataDictionary	dd,
		TableDescriptor actualTd,  // the table we are adding the foriegn key.
		TableDescriptor	refTd,
		int refActionType,
		Hashtable dch,
		Hashtable ech,  //existing delete connections
		boolean checkImmediateRefTable,
		String myConstraintName,
		boolean prevNotCascade,
		StringBuffer cycleString, 
		String currentRefTableName, //the name of the table we are referring too.
		boolean isSelfReferencingFk,
		int currentSelfRefValue
		)
		throws StandardException
	{

		Integer rAction;

		String refTableName = refTd.getSchemaName() + "." + refTd.getName();


		/*
		** Validate the new referentail action value with respect to the 
		** already existing connections to this table we gathered  from
		** the getCurrentDeleteConnections() call.
		*/

		if(checkImmediateRefTable)
		{
			rAction = ((Integer)dch.get(refTableName));
			
			// check possible invalide cases incase of self referencing foreign key
			if(isSelfReferencingFk)
			{
				//All the relation ship referring to a table should have the
				//same refaction except incase of SET NULL. In this case
				//it is the same table , so we have to check with existing self
				//referencing actions.
				if(currentSelfRefValue !=  -1)
				{
					if(currentSelfRefValue != refActionType)
					{
						//If there is a SET NULL relation ship we can not have any
						// other relation ship with it.
						if(currentSelfRefValue == StatementType.RA_SETNULL)
							throw
								generateError(SQLState.LANG_CANT_BE_DEPENDENT_ESELF, 
										  myConstraintName, currentRefTableName);
						else
						{
								/*
								** case where we can cleary say what the
								** referential actions should be. Like,	   
								** if there is NO ACTION relationsip
								**already, new relation ship also shold be NO ACTION. 
								*/
							throw
								generateError(SQLState.LANG_DELETE_RULE_MUSTBE_ESELF,
											  myConstraintName, currentSelfRefValue);
						}
					}else
					{
						//more than one  ON DELET SET NULL to the same table is not allowed
						if(currentSelfRefValue == StatementType.RA_SETNULL &&
						   refActionType == StatementType.RA_SETNULL)
						{
							throw
								generateError(SQLState.LANG_CANT_BE_DEPENDENT_ESELF,
											  myConstraintName, currentRefTableName);
						}
					}
				}

				/*
				** If the new releation ship is self referencing and if
				** the current existing relation ship to other tables is
				** CASCADE type them  new self reference should be of type
				** CASCADE, otherwise we should throw error.
				*/

				if(isSelfReferencingFk && dch.contains(new Integer(StatementType.RA_CASCADE)) && 
				   refActionType!=  StatementType.RA_CASCADE)
				{
					throw
						generateError(SQLState.LANG_DELETE_RULE_MUSTBE_ECASCADE,
									  myConstraintName,StatementType.RA_CASCADE);	
				}

				//end of possible error case scenarios for self reference key additions
				return;
			}
		
			//cases where the new  reference is referring to  another table

			//check whether it matched with existing self references.
			// If A self-referencing constraint exists with a delete rule of
			// SET NULL,  NO ACTION or RESTRICT. We can not add CASCADE
			// relationship with another table.
				
			if(currentSelfRefValue !=  -1)
			{
				if(refActionType == StatementType.RA_CASCADE && 
				   currentSelfRefValue != StatementType.RA_CASCADE) 
				{
					throw generateError(SQLState.LANG_DELETE_RULE_CANT_BE_CASCADE_ESELF,  myConstraintName);
					
				}

			}

			
			//check for the cases with existing relationships to the
			//referenced table
			if(rAction != null)
			{
				checkForMultiplePathInvalidCases(rAction.intValue(),
												  refActionType,
												  myConstraintName,currentRefTableName);
			}

			
			//mark the current connect to the reference table to identify the cycle.
			if(refActionType != StatementType.RA_CASCADE)
			{
				prevNotCascade = true;
			}
			
			/*
			** cycle string is used to keep track of the referential actions of 
			** the nodes we visited, this is required to make sure that in case
			** of cycles , all the nodes in the cycle have same type of
			** referential action.
			**/
			cycleString = cycleString.append(refActionType);
		}


		boolean passedInPrevNotCascade = prevNotCascade;

		//delete connection is broken for if we see ON DELET SET NULL link
		// one level deeper than the table we are adding the foreing key
		//Where as to check for cycles we need to go for more level also;
        // To check cases like CASCADE CASCADE SET NULL cycle is not valid. 
		//Following variable is used make the distinction.
		boolean multiPathCheck = true;

		// check for cases where the new connection we are forming to the 
		// reference table could create invalid any cycles or mutiple paths 
		// with the delete-connections the  referencing table might have already.
		ConstraintDescriptorList refCDL = dd.getConstraintDescriptors(refTd);
		int refCDLSize = refCDL.size();
		for (int index = 0; index < refCDLSize; index++)
		{
			ConstraintDescriptor cd = refCDL.elementAt(index);

			if ((cd instanceof ForeignKeyConstraintDescriptor))
			{
				ForeignKeyConstraintDescriptor fkcd = (ForeignKeyConstraintDescriptor) cd;
				String constraintName = fkcd.getConstraintName();
				int raDeleteRule = fkcd.getRaDeleteRule();
				int raUpdateRule = fkcd.getRaUpdateRule();
				
				ReferencedKeyConstraintDescriptor refcd =
					fkcd.getReferencedConstraint(); 
				TableDescriptor nextRefTd = refcd.getTableDescriptor();

				//if we are not cascading, check  whether the link before
				//this was cascade or not. If we travel through  two NON CASCADE ACTION
				//links then the delete connection is broken(only a delete can have further
				//referential effects)
				if(raDeleteRule != StatementType.RA_CASCADE)
				{
					if(prevNotCascade)
					{
						prevNotCascade = passedInPrevNotCascade;
						continue;
					}
					else
					{
						prevNotCascade = true;
						multiPathCheck = false;
					}

				}

				//check whether the current link is a self referencing one
				boolean isSelfRefLink = fkcd.isSelfReferencingFK();
				
				//check for this is non self referencing cycles case
				//In cases of cycle, whole cycle should have the same refAction
				// value. Other wise we should throw an exception
				cycleString = cycleString.append(raDeleteRule);
				boolean isFormingCycle = (nextRefTd.getUUID().equals(actualTd.getUUID()));
				if(isFormingCycle)
				{
					//make sure that all the nodes in the cycle have the same
					//referential action  value, otherwise we should throw an error. 
					for(int i = 0 ; i < cycleString.length(); i++)
					{
						int otherRefAction = Character.getNumericValue(cycleString.charAt(i));
						if(otherRefAction != refActionType)
						{
							//cases where one of the existing relation ships in
							//the cycle is not cascade , so we can not have
							// cascade relation ship.
							if(otherRefAction != StatementType.RA_CASCADE)
							{
								throw generateError(SQLState.LANG_DELETE_RULE_CANT_BE_CASCADE_ECYCLE, myConstraintName);
							}
							else
							{
								//possibly all the other nodes in the cycle has
								//cascade relationsship , we can not add a non
								//cascade relation ship.
								throw
									generateError(SQLState.LANG_CANT_BE_DEPENDENT_ECYCLE, 
												  myConstraintName, currentRefTableName);
							}
						}
					}
				}	


				

				String nextRefTableName =  nextRefTd.getSchemaName() + "." + nextRefTd.getName();
				rAction = ((Integer)ech.get(nextRefTableName));
				if(rAction != null)
				{
					/*
					** If the table name has entry in the hash table means, there
					** is already  a path to this table exists from the table
					** the new foreign key relation ship is being formed.
					** Note: refValue in the hash table is how the table we are
					** adding the new relationsship is going to affected not
					** current path refvalue.
					**/
					if(!isSelfRefLink && multiPathCheck)
						checkForMultiplePathInvalidCases(rAction.intValue(),
														 refActionType, 
														 myConstraintName,currentRefTableName);

				}else
				{
					rAction = ((Integer)dch.get(nextRefTableName));
					if(rAction == null)
					{
						if(multiPathCheck)
							dch.put(nextRefTableName, (new Integer(refActionType)));
						if(!isSelfRefLink)
						{
							validateDeleteConnection(dd, actualTd,  nextRefTd,
												 refActionType, dch, ech, false,
												 myConstraintName,prevNotCascade,
												 cycleString, currentRefTableName, 
												 isSelfReferencingFk, currentSelfRefValue); 
						}
					}
				}
				prevNotCascade = passedInPrevNotCascade;
				//removes the char added for the current call
				cycleString.setLength(cycleString.length() -1);
				
			}
		}
	}


	/*
	**Check whether the mulitple path case is valid or not following
	** cases are invalid:
	** case 1: The relationship causes the table to be delete-connected to
	** the indicated table through multiple relationships and the
	** delete rule of the existing relationship is SET NULL. 
	** case 2: The relationship would cause the table to be
	** delete-connected to the same table through multiple
	** relationships and such relationships must have the same 
	** delete rule (NO ACTION, RESTRICT or CASCADE). 
	** case 3: The relationship would cause another table to be
	** delete-connected to the same table through multiple paths
	** with different delete rules or with delete rule equal to SET NULL. 
	**/

	private static void checkForMultiplePathInvalidCases(int currentRefAction,
														  int refActionType,
														  String myConstraintName,
														  String currentRefTableName)
		throws StandardException
	{

		//All the relation ship referring to a table should have the
		//same refaction except incase of SET NULL
		if(currentRefAction != refActionType)
		{

			//If there is a SET NULL relation ship we can not have any
			// other relation ship with it.
			if(currentRefAction == StatementType.RA_SETNULL)
				throw generateError(SQLState.LANG_CANT_BE_DEPENDENT_MPATH,
									myConstraintName, currentRefTableName);
			else
				//This error say what the delete rule must be for the
				// foreign key be valid 
				throw generateError(SQLState.LANG_DELETE_RULE_MUSTBE_MPATH,
									myConstraintName, currentRefAction);

		}else
		{
			//more than one  ON DELET SET NULL to the same table is not allowed
			if(currentRefAction == StatementType.RA_SETNULL &&
			   refActionType == StatementType.RA_SETNULL)
			{
				throw		
					generateError(SQLState.LANG_CANT_BE_DEPENDENT_MPATH,
								  myConstraintName, currentRefTableName);
			}
		}
	}



    /*
	** Check whether the delete rule of FOREIGN KEY  must not be CASCADE because
	** the  new relationship would cause another table to be delete-connected to
	** the same table through multiple paths with different delete rules or with 
	** delete rule equal to SET NULL. 
	**
	** For example :
	**                      t1
    **  		 CASCADE   /  \  CASCADE
    **                    /    \ 
	**                  t2      t3
    **                   \      /   
    **          SET NULL  \    /  CASCADE (Can we add this one ? NO)
 	**			          \   /
	                       \t4/
	**					
    **   existing links:
	**   t2 references t1   ON DELETE CASCADE  (fkey1)
	**   t3 references t1   ON DELETE CASCADE  (fkey2)
	**   t2 reference  t4   ON DELETE SET NULL (fkey3)
	**   Now if if try to add a new link i.e
	**   t4 references t3   ON DELETE SET NULL  (fkey4)
	**   Say if we add it,  then if we execute 'delete from t1' 
	**   Because of referential actions , we will try to delete a row through
	**   one path and tries to update  through another path. 
	**   Nothing in standard that say whether we are suppose to delete the row
	**   or update the row.  DB2UDB raises error when we try to create the
	**   foreign key fkey4, cloudscape also does the same.
	** 
	**   How we catch the error case ?
	**   Point to note here is the table(t4) we are  adding the foreign key does
	**   not have a problem in this scenarion because we are adding a
    **   a CASACDE link , some other table(t2) that is referring  
	**   can get multiple referential action paths. We can not
	**   this error case for self referencing links.
	**   Algorithm:
	**   -Gather the foreign keys that are
	**   referring(ReferencedKeyConstraintDescriptor) to the table we are adding
	**   foreign key, in our example case we get (fkey3 - table t2 -t4 link)
	**   for each ReferencedKeyConstraintDescriptor
	**   {
	**    1)find the delete connections of the referring table.
	**    [getCurrentDeleteConnections() will return this hash table]
	**	  2) we already have collected the Delete connections 
    **       in validDeleteConnections() for the actual table we are adding the 
    **       foreign key.
	**    3) Now check whether the referring table  is also 
    **       referring  any table that the table we are adding
    **       foreign key has delete connection.
	**
	**     for each table referring table delete connection hash table
	**     {
	**      if it is there in the actual table delete connection hash table
    **      {
	**         //In our example case we find t1 in both the hash tables.
	**         make sure we are having valid referential action
    **         from the existing path and the new path we got from 
	**         new foreign key relation ship.
	**        //In our example case t2 has CASCADE relations with t1
	**        //Because of new foreign key added we also get 
	**        //SET NULL relation ship with t1. This is not valid
	**        //so we should throw error.
    **      }  
	**     }
	** }	
	**/


	private static void checkForAnyExistingDeleteConnectionViolations
	(
	 DataDictionary	dd,
	 TableDescriptor td,
	 int refActionType,
	 Hashtable newDconnHashTable,
	 String myConstraintName
	 )
	throws StandardException
	{

		//We need to check for the condition in this function only when we are
		//adding ref action of type CASCADE
		if(refActionType != StatementType.RA_CASCADE)
			return;
		
		//find the tables that are referring to the table we 
		//are adding the foreign key and check whether we violate their existing rules.
		String addTableName = td.getSchemaName() + "." + td.getName();;
		ConstraintDescriptorList refCDL = dd.getConstraintDescriptors(td);
		int refCDLSize = refCDL.size();
		for (int index = 0; index < refCDLSize; index++)
		{
			ConstraintDescriptor cd = refCDL.elementAt(index);

			if ((cd instanceof ReferencedKeyConstraintDescriptor))
			{
				ConstraintDescriptorList fkcdl = dd.getActiveConstraintDescriptors
					( ((ReferencedKeyConstraintDescriptor)cd).getForeignKeyConstraints(ConstraintDescriptor.ALL));
	
				int size = fkcdl.size();
				if (size == 0) 
				{ 
					continue; 
				}
				
				//Note: More than one table can refer to the same
				//ReferencedKeyConstraintDescriptor, so we need to find all the tables.
				Hashtable dConnHashtable = new Hashtable();
				for (int inner = 0; inner < size; inner++)
				{
					ForeignKeyConstraintDescriptor fkcd = (ForeignKeyConstraintDescriptor) fkcdl.elementAt(inner);
					TableDescriptor fktd = fkcd.getTableDescriptor();
					//Delete rule that we have to the table we are adding the
					// foreign key relation shop
					int raDeleteRuleToAddTable = fkcd.getRaDeleteRule();

					//This check should not be done on self referencing references.
					if(!fkcd.isSelfReferencingFK())
					{

						//gather the delete connections of the table that is
						//referring to the table we are adding foreign key relation ship

						getCurrentDeleteConnections(dd, fktd, -1, dConnHashtable, false, true);

						/*
						**Find out if we introduced more than one delete connection
						**paths to the table that are referring the table we adding
						**the foreign key relatiosn ship.
						**If we have multiple paths they should have the same type
						**referential action and only one SET NULL path.
						**/

						for (Enumeration e = dConnHashtable.keys() ; e.hasMoreElements() ;) 
						{
							String tName = (String) e.nextElement();
							//we should not check for the table name to which  we are
							//adding the foreign key relation ship.
							if(!tName.equals(addTableName))
							{
								if(newDconnHashTable.containsKey(tName))
								{
									int currentDeleteRule = ((Integer)	dConnHashtable.get(tName)).intValue();
									if((currentDeleteRule == StatementType.RA_SETNULL
										&& raDeleteRuleToAddTable == StatementType.RA_SETNULL) ||
									   currentDeleteRule  != raDeleteRuleToAddTable)
									{
										throw
											generateError(SQLState.LANG_DELETE_RULE_CANT_BE_CASCADE_MPATH, 
														  myConstraintName);
									}
								}
							}
						}
					}
					//same hash table can be used for the other referring tables
					//so clear the hash table.
					dConnHashtable.clear();
				}
			}
		}
	}


	
	private static StandardException generateError(String messageId, 
												   String myConstraintName)
	{
		String message = MessageService.getTextMessage(messageId);
		return StandardException.newException(SQLState.LANG_DELETE_RULE_VIOLATION, 
												myConstraintName, message);
	}	

	private static StandardException generateError(String messageId, 
												   String myConstraintName, 
												   int raRule)
	{
		String raRuleStringId;
		switch (raRule){
		case StatementType.RA_CASCADE:
			raRuleStringId = SQLState.LANG_DELETE_RULE_CASCADE;
			break;
		case StatementType.RA_RESTRICT:
			raRuleStringId = SQLState.LANG_DELETE_RULE_RESTRICT;
				break;
		case StatementType.RA_NOACTION:
			raRuleStringId = SQLState.LANG_DELETE_RULE_NOACTION;
			break;
		case StatementType.RA_SETNULL:
			raRuleStringId = SQLState.LANG_DELETE_RULE_SETNULL;
			break;
		case StatementType.RA_SETDEFAULT:
			raRuleStringId = SQLState.LANG_DELETE_RULE_SETDEFAULT;
			break;
		default: 
			raRuleStringId =SQLState.LANG_DELETE_RULE_NOACTION ; // NO ACTION (default value)
		}

		String raRuleMessageString = MessageService.getTextMessage(raRuleStringId); 
		String message = MessageService.getTextMessage(messageId, raRuleMessageString);
		return StandardException.newException(SQLState.LANG_DELETE_RULE_VIOLATION, 
												myConstraintName, message);
	}	

	private static StandardException generateError(String messageId, 
												   String myConstraintName,
												   String refTableName)
	{

		String message = MessageService.getTextMessage(messageId, refTableName);
		return StandardException.newException(SQLState.LANG_DELETE_RULE_VIOLATION, 
												myConstraintName, message);
	}	

}















