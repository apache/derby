/*

   Derby - Class org.apache.derby.iapi.sql.compile.Visitable

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

package org.apache.derby.iapi.sql.compile;

import java.util.List;

import org.apache.derby.shared.common.error.StandardException;

/**
 * A Visitable is something that can be visited by
 * a Visitor
 *
 */
public interface Visitable
{
	/**
	 * Accept a visitor, and call v.visit()
	 * on child nodes as necessary.  
	 * 
	 * @param v the visitor
	 *
	 * @exception StandardException on error
	 */
	public  Visitable accept(Visitor v) 
		throws StandardException;

    /**
     * Add a tag to this Visitable.
     */
    public  void    addTag( String tag );

    /**
     * Return true if this Visitable is tagged with the indicated tag.
     */
    public  boolean taggedWith( String tag );
}
