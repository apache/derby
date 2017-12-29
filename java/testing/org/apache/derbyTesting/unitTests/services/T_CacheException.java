/*

   Derby - Class org.apache.derbyTesting.unitTests.services.T_CacheException

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

package org.apache.derbyTesting.unitTests.services;

import org.apache.derby.shared.common.error.StandardException;

/**

*/
public  class T_CacheException extends T_StandardException {

	public static final int		ERROR = 0;
	public static final int     INVALID_KEY = 1;
	public static final int		IDENTITY_FAIL = 2;

	protected int type;

		
	protected T_CacheException(String message, int type) {
		super("cache.S", message);
		this.type = type;
	}

	public static StandardException invalidKey() {
		return new T_CacheException("invalid key passed", INVALID_KEY);
	}
	public static StandardException identityFail() {
		return new T_CacheException("identity change failed", IDENTITY_FAIL);
	}

	protected int getType() {
		return type;
	}
}

