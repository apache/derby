/*

   Derby - Class org.apache.derby.impl.drda.DRDAProtocolExceptionInfo

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.drda;

public class DRDAProtocolExceptionInfo {
    
    /*
      Holds static information about the protocol error
      to put in the Hash Table
    */
    // The Codepoint of the error (e.g CodePoint.SYTNAXRM)
    protected int errorCodePoint;	   
    
    // Severity Code
    protected int svrcod;
    
    // The CodePoint describing the errCD (e.g. CodePint.SYNERRCD)
    protected int errCdCodePoint ;
    
    // Sends an originating Codepoint
    protected boolean sendsCodpntArg;
	
    protected DRDAProtocolExceptionInfo(int errorCodePoint, int svrcod,  
					int errCdCodePoint,
					boolean sendsCodpntArg)
    {
	this.errorCodePoint = errorCodePoint;
	this.svrcod = svrcod;
	this.errCdCodePoint = errCdCodePoint;
	this.sendsCodpntArg = sendsCodpntArg;
    }
    
    
}

















