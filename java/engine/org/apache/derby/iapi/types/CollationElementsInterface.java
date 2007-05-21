/*

   Derby - Class org.apache.derby.iapi.types.CollationElementsInterface
 
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

import org.apache.derby.iapi.error.StandardException;

/**
 * CollationElementsInterface is an interface which will be implemented by  
 * all the Collator sensitive char data types. These methods will be called by 
 * WorkHorseForCollatorDatatypes's collation sensitive methods 
 * "like, stringcompare" etc.  
 */
interface CollationElementsInterface
{
	/**
	 * This method translates the string into a series of collation elements.
	 * These elements will get used in the like method.
	 * 
	 * @return an array of collation elements for the string
	 * @throws StandardException
	 */
	public int[] getCollationElementsForString() throws StandardException; 

	/**
	 * This method returns the count of collation elements for this instance of
	 * CollationElementsInterface. This method will return the correct value only if  
	 * method getCollationElementsForString has been called previously on this 
	 * instance of CollationElementsInterface. 
	 *
	 * @return count of collation elements for this instance of CollatorSQLChar
	 */
	public int getCountOfCollationElements();
}
