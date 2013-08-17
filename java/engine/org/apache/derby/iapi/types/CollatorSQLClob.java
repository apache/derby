/*

   Derby - Class org.apache.derby.iapi.types.CollatorSQLClob
 
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

package org.apache.derby.iapi.types;

import java.sql.Clob;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.sanity.SanityManager;

import java.text.RuleBasedCollator;

/**
 * CollatorSQLClob class differs from SQLClob based on how the 2 classes  
 * use different collations to collate their data. SQLClob uses Derby's 
 * default collation which is UCS_BASIC. Whereas, this class uses the 
 * RuleBasedCollator object that was passed to it in it's constructor and that 
 * RuleBasedCollator object decides the collation.
 * 
 * In Derby 10.3, this class will be passed a RuleBasedCollator which is based 
 * on the database's territory. In future releases of Derby, this class can be 
 * used to do other kinds of collations like case-insensitive collation etc by  
 * just passing an appropriate RuleBasedCollator object for that kind of 
 * collation.
 */
class CollatorSQLClob extends SQLClob implements CollationElementsInterface
{
	private WorkHorseForCollatorDatatypes holderForCollationSensitiveInfo;

	/*
	 * constructors
	 */
    
    /**
     * Create SQL CLOB value initially set to NULL that
     * performs collation according to collatorForCharacterDatatypes 
     */
    CollatorSQLClob(RuleBasedCollator collatorForCharacterDatatypes)
    {
        setCollator(collatorForCharacterDatatypes);
    }
    
    /**
     * Create SQL CLOB value initially set to value that
     * performs collation according to collatorForCharacterDatatypes 
     */
	CollatorSQLClob(String val, RuleBasedCollator collatorForCharacterDatatypes)
	{
		super(val);
        setCollator(collatorForCharacterDatatypes);
	}

    /**
     * Create SQL CLOB value initially set to value that
     * performs collation according to collatorForCharacterDatatypes 
     */
	CollatorSQLClob(Clob val, RuleBasedCollator collatorForCharacterDatatypes)
	{
		super(val);
        setCollator(collatorForCharacterDatatypes);
	}

	/**
	 * Set the RuleBasedCollator for this instance of CollatorSQLClob. It will
	 * be used to do the collation.
	 */
	private void setCollator(RuleBasedCollator collatorForCharacterDatatypes)
	{
		holderForCollationSensitiveInfo = 
			new WorkHorseForCollatorDatatypes(collatorForCharacterDatatypes, this);
	}

	/**
	 * Get the RuleBasedCollator for this instance of CollatorSQLClob. It 
	 * will be used to do the collation.
	 * 
	 * @return	The Collator object which should be used for collation 
	 * operation on this object
	 */
	protected RuleBasedCollator getCollatorForCollation() throws StandardException
	{
		return holderForCollationSensitiveInfo.getCollatorForCollation();
	}
	
    // Implementation of CollationElementsInterface interface

    public boolean hasSingleCollationElement() throws StandardException {
        return holderForCollationSensitiveInfo.hasSingleCollationElement();
    }

	/*
	 * DataValueDescriptor interface
	 */

    /**
     * @see DataValueDescriptor#cloneValue
     */
    public DataValueDescriptor cloneValue(boolean forceMaterialization)
	{
        if (forceMaterialization) {
            try {
                return new CollatorSQLClob(getString(),
                    holderForCollationSensitiveInfo.getCollatorForCollation());
            }
            catch (StandardException se)
            {
                if (SanityManager.DEBUG)
                    SanityManager.THROWASSERT("Unexpected exception", se);
                return null;
            }
        } else {
            // If materialization isn't forced, let SQLClob (super) decide how
            // to clone the value.
           SQLClob clob = (SQLClob)super.cloneValue(forceMaterialization);
           CollatorSQLClob clone = new CollatorSQLClob(
                   holderForCollationSensitiveInfo.getCollatorForCollation());
           clone.copyState(clob);
           return clone;
        }
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		CollatorSQLClob result = new CollatorSQLClob((String) null,
				holderForCollationSensitiveInfo.getCollatorForCollation());
		return result;
	}

	/**
	 * We do not anticipate this method on collation sensitive DVD to be
	 * ever called in Derby 10.3 In future, when Derby will start supporting
	 * SQL standard COLLATE clause, this method might get called on the
	 * collation sensitive DVDs.
	 *  
	 * @see StringDataValue#getValue(RuleBasedCollator) 
	 */
	public StringDataValue getValue(RuleBasedCollator collatorForComparison)
	{
		if (collatorForComparison != null)
		{
			//non-null collatorForComparison means use this collator sensitive
			//implementation of SQLClob
		    setCollator(collatorForComparison);
		    return this;			
		} else {
			//null collatorForComparison means use UCS_BASIC for collation.
			//For that, we need to use the base class SQLClob
			SQLClob s = new SQLClob();
			s.copyState(this);
			return s;
		}
	}
	
	/** @see SQLChar#stringCompare(SQLChar, SQLChar) */
	 protected int stringCompare(SQLChar char1, SQLChar char2)
	 throws StandardException
	 {
		 return holderForCollationSensitiveInfo.stringCompare(char1, char2);
	 }

     /**
      * Return a hash code that is consistent with
      * {@link #stringCompare(SQLChar, SQLChar)}.
      *
      * @return hash code
      */
     public int hashCode() {
         return hashCodeForCollation();
     }

	/**
	 * This method implements the like function for char (with no escape value).
	 * The difference in this method and the same method in superclass is that
	 * here we use special Collator object to do the comparison rather than
	 * using the Collator object associated with the default jvm locale.
	 *
	 * @param pattern		The pattern to use
	 *
	 * @return	A SQL boolean value telling whether the first operand is
	 *			like the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */
	public BooleanDataValue like(DataValueDescriptor pattern)
								throws StandardException
	{
		return(holderForCollationSensitiveInfo.like(pattern));
	}
	
	/**
	 * This method implements the like function for char with an escape value.
	 * 
	 * @param pattern		The pattern to use
	 * 								 
	 * @return	A SQL boolean value telling whether the first operand is
	 * like the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */
	public BooleanDataValue like(DataValueDescriptor pattern,
			DataValueDescriptor escape) throws StandardException
	{
		return(holderForCollationSensitiveInfo.like(pattern, escape));
	}
}
