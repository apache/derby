/*

   Derby - Class org.apache.derby.impl.tools.ij.ijFatalException

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

package org.apache.derby.impl.tools.ij;

import org.apache.derby.iapi.tools.i18n.LocalizedResource;
import java.io.IOException;
import java.sql.SQLException;
/**
 * Used for fatal IJ exceptions
 */

public class ijFatalException extends RuntimeException {

	private final static String FatalException = LocalizedResource.getMessage("IJ_FataExceTerm");
	private SQLException e;

	public ijFatalException() 
	{
		super(FatalException);
		e = null;
	}

	public ijFatalException(SQLException e) 
	{
		super(FatalException); 
		this.e = e;
	}

	public String getSQLState()
	{
		return e.getSQLState();
	}
	
	public String toString()
	{
		return LocalizedResource.getMessage("IJ_Fata01",e.getSQLState(),e.getMessage());
	}
}
