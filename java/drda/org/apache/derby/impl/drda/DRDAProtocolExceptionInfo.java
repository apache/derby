/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.drda
   (C) Copyright IBM Corp. 2002, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.drda;

public class DRDAProtocolExceptionInfo {
	/**
		IBM Copyright &copy notice.
	*/


    private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2002_2004;
    
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

















