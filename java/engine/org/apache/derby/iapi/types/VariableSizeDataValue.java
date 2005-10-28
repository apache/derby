/*

   Derby - Class org.apache.derby.iapi.types.VariableSizeDataValue

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.types;

import org.apache.derby.iapi.error.StandardException;

/**
 * The VariableSizeDataValue interface corresponds to 
 * Datatypes that have adjustable width. 
 *
 * The following methods are defined herein:
 *		setWidth()
 *
 * @author	jamie
 */
public interface VariableSizeDataValue 
{

	public static int IGNORE_PRECISION = -1;

	/*
	 * Set the width and scale (if relevant).  Sort of a poor
	 * man's normalize.  Used when we need to normalize a datatype
	 * but we don't want to use a NormalizeResultSet (e.g.
	 * for an operator that can change the width/scale of a
	 * datatype, namely CastNode).
	 *
	 * @param desiredWidth width
	 * @param desiredScale scale, if relevant (ignored for strings)
	 * @param errorOnTrunc	throw an error on truncation of value
	 *
	 * @exception StandardException		Thrown on error
	 */
	public DataValueDescriptor setWidth(int desiredWidth,
									int desiredScale,
									boolean errorOnTrunc)
							throws StandardException;
}
