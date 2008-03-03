/*

   Derby - Class org.apache.derby.impl.sql.compile.RowNumberColumnNode

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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.TypeId;

import java.sql.Types;

public final class RowNumberColumnNode extends WindowFunctionColumnNode
{
	
	/**
	 * Initializer for a RowNumberColumnNode
	 *
	 * @exception StandardException
	 */
	public void init()
		throws StandardException
	{
		super.init();
		setType( TypeId.getBuiltInTypeId( Types.BIGINT ),
				 TypeId.LONGINT_PRECISION,
				 TypeId.LONGINT_SCALE, 
				 false,
				 TypeId.LONGINT_MAXWIDTH);			
	}
        
	/**
	 * Initializer for a RowNumberColumn node
	 *
	 * @param arg1 The window definition
	 *
	 * @exception StandardException
	 */
	public void init(Object arg1)
		throws StandardException
	{
		this.init();		
		setWindowNode((WindowNode) arg1);
	}
            
	public boolean isEquivalent(ValueNode o) throws StandardException
	{
        /* Two RowNumberColumnNodes should never be equivalent */
        return false;
	}

	/**
	 * Indicate whether this column is ascending or not.
	 * By default assume that all ordered columns are
	 * necessarily ascending.  If this class is inherited
	 * by someone that can be desceneded, they are expected
	 * to override this method.
	 *
	 * @return true
	 */
	public boolean isAscending()
	{
		return true;
	}
}		
