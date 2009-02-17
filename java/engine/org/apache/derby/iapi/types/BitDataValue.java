/*

   Derby - Class org.apache.derby.iapi.types.BitDataValue

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

import java.sql.Blob;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.iapi.services.io.StreamStorable;

/*
 * The BitDataValue interface corresponds to a SQL BIT 
 */

public interface BitDataValue extends ConcatableDataValue, StreamStorable
{
	/**
	 * The SQL concatenation '||' operator.
	 *
	 * @param leftOperand	String on the left hand side of '||'
	 * @param rightOperand	String on the right hand side of '||'
	 * @param result	The result of a previous call to this method,
	 *					null if not called yet.
	 *
	 * @return	A ConcatableDataValue containing the result of the '||'
	 *
	 * @exception StandardException		Thrown on error
	 */
	public BitDataValue concatenate(
				BitDataValue leftOperand,
				BitDataValue rightOperand,
				BitDataValue result)
		throws StandardException;

	/**
	 * Stuff a BitDataValue with a Blob.
	 */
	public void setValue( Blob value )
		throws StandardException;

}
