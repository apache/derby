/*

   Derby - Class org.apache.derby.catalog.AliasInfo

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

package org.apache.derby.catalog;

/**
 *
 * An interface for describing an alias in Derby systems.
 * 
 * In a Derby system, an alias can be one of the following:
 * <ul>
 * <li>method alias
 * <li>class alias
 * <li>synonym
 * <li>user-defined aggregate
 * </ul>
 *
 */
public interface AliasInfo
{
	/**
	 * Public statics for the various alias types as both char and String.
	 */
	public static final char ALIAS_TYPE_PROCEDURE_AS_CHAR		= 'P';
	public static final char ALIAS_TYPE_FUNCTION_AS_CHAR		= 'F';
	public static final char ALIAS_TYPE_SYNONYM_AS_CHAR             = 'S';	

	public static final String ALIAS_TYPE_PROCEDURE_AS_STRING		= "P";
	public static final String ALIAS_TYPE_FUNCTION_AS_STRING		= "F";
	public static final String ALIAS_TYPE_SYNONYM_AS_STRING  		= "S";

	/**
	 * Public statics for the various alias name spaces as both char and String.
	 */
	public static final char ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR	= 'P';
	public static final char ALIAS_NAME_SPACE_FUNCTION_AS_CHAR	= 'F';
	public static final char ALIAS_NAME_SPACE_SYNONYM_AS_CHAR       = 'S';

	public static final String ALIAS_NAME_SPACE_PROCEDURE_AS_STRING	= "P";
	public static final String ALIAS_NAME_SPACE_FUNCTION_AS_STRING	= "F";
	public static final String ALIAS_NAME_SPACE_SYNONYM_AS_STRING   = "S";

	/**
	 * Get the name of the static method that the alias 
	 * represents at the source database.  (Only meaningful for
	 * method aliases )
	 *
	 * @return The name of the static method that the alias 
	 * represents at the source database.
	 */
	public String getMethodName();

	/**
	 * Return true if this alias is a Table Function.
	 */
	public boolean isTableFunction();

}
