/*

   Derby - Class org.apache.derby.impl.drda.AppRequester

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
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.reference.DB2Limit;

/**
	AppRequester stores information about the application requester.
	It is used so that multiple sessions can share information when they are
	started from the same version of the application requester.
*/
class AppRequester
{

	protected static final int MGR_LEVEL_UNKNOWN = -1;

	protected static final int UNKNOWN_CLIENT = 0;
	protected static final int JCC_CLIENT = 1;
	protected static final int CCC_CLIENT = 2;		// not yet supported.

	private static final int [] MIN_MGR_LEVELS = {
											3, // AGENT - JCC comes in at 3
											4, // CCSIDMGR	
											3, // CMNAPPC, 
											4, // CMNSYNCPT
											5, // CMNTCPIP
											1, // DICTIONARY
											3, // RDB
											4, // RSYNCMGR
											1, // SECMGR	
											6, // SQLAM
											1, // SUPERVISOR	
											5, // SYNCPTMGR
											7  // XAMGR
											};
	// Application requester information
	protected String	extnam;			// External Name - EXCSAT
	protected String	srvnam;			// Server Name - EXCSAT
	protected String 	srvrlslv;		// Server Product Release Level - EXCSAT
	protected String	srvclsnm;		// Server Class Name - EXCSAT
	protected String	spvnam;			// Supervisor Name - EXCSAT
	protected String	prdid;			// Product specific identifier - ACCRDB protected
	private int[]		managerLevels = new int[CodePoint.MGR_CODEPOINTS.length];
	private int 		clientType;
	protected int		versionLevel;
	protected int		releaseLevel;
	protected int		modifyLevel;
	

	// constructor 
	/** 
	 * AppRequester constructor
	 * 
	 * @exception throws IOException
	 */
	protected AppRequester () 
	{
		for (int i = 0; i < CodePoint.MGR_CODEPOINTS.length; i++)
			managerLevels[i] = MGR_LEVEL_UNKNOWN;
	}

	/**
	 * get the Application requester manager level
	 *
	 * @param manager	codepoint for manager we are looking for
	 *
	 * @return manager level for that manager
	 */
	protected int getManagerLevel(int manager)
	{
		int mindex = CodePoint.getManagerIndex(manager);
		if (SanityManager.DEBUG)
		{
			if (mindex < 0 || mindex > managerLevels.length)
				SanityManager.THROWASSERT("Unknown manager "+ manager + " mindex = "+
					mindex);
		}
		return managerLevels[mindex];
	}

	protected void setClientVersion(String productId)
	{
		prdid = productId;

		versionLevel = Integer.parseInt(prdid.substring (3, 5));
		releaseLevel = Integer.parseInt(prdid.substring (5, 7));
		modifyLevel = Integer.parseInt(prdid.substring (7, 8));
		if (srvrlslv == null)
			clientType = UNKNOWN_CLIENT;
		else if (srvrlslv.indexOf("JCC") != -1)
			clientType = JCC_CLIENT;
		else
			clientType = UNKNOWN_CLIENT;
	}

	/**
	 * Check if provided JCC version level is greaterThanOrEqualTo current level
	 *
	 * @param vLevel	Version level
	 * @param rLevel	Release level
	 * @param mLevel	Modification level
	 */
	 
	protected boolean greaterThanOrEqualTo(int vLevel, int rLevel, int mLevel)
	{
		if (versionLevel > vLevel)
				return true;
		else if (versionLevel == vLevel) {
				if (releaseLevel > rLevel)
						return true;
				else if (releaseLevel == rLevel)
						if (modifyLevel >= mLevel)
								return true;
		}
    	return false;
	}

	/** 
	 * set Application requester manager level
	 * if the manager level is less than the minimum manager level,
	 * set the manager level to zero (saying we can't handle this
	 * level), this will be returned
	 * to the application requester and he can decide whether or not to
	 * proceed
	 * For CCSIDMGR, if the target server supports the CCSID manager but
	 * not the CCSID requested, the value returned is FFFF
	 * For now, we won't support the CCSIDMGR since JCC doesn't request it.
	 *
	 * @param manager	codepoint of the manager
	 * @param managerLevel	level for that manager
	 *
	 */
	protected void setManagerLevel(int manager, int managerLevel)
	{
		int i = CodePoint.getManagerIndex(manager);
		if (SanityManager.DEBUG)
		{
			if (i < 0 || i > managerLevels.length)
				SanityManager.THROWASSERT("Unknown manager "+ manager + " i = " + i);
		}
		if (managerLevel >= MIN_MGR_LEVELS[i])
			managerLevels[i] = managerLevel;	
		else
			managerLevels[i] = 0;
	}
	
	/**
	 * Check if the application requester is the same as this one
	 *
	 * @param a	application requester to compare to
	 * @return true if same false otherwise
	 */
	protected boolean equals(AppRequester a)
	{
		// check prdid - this should be different if they are different
		if (!prdid.equals(a.prdid))
			return false;

		// check server product release level
		if (notEquals(srvrlslv, a.srvrlslv))
			return false;

		// check server names
		if (notEquals(extnam, a.extnam))
			return false;

		if (notEquals(srvnam, a.srvnam))
			return false;

		if (notEquals(srvclsnm, a.srvclsnm))
			return false;

		if (notEquals(spvnam, a.spvnam))
			return false;

		// check manager levels
		for (int i = 0; i < managerLevels.length; i++)
			if (managerLevels[i] != a.managerLevels[i])
				return false;

		// O.K. looks good
		return true;
	}
	/**
	 * Check whether two objects are not equal when 1 of the objects could
	 * be null
	 *
 	 * @param a	first object
	 * @param b second object
	 * @return true if not equals false otherwise
	 */
	private boolean notEquals(Object a, Object b)
	{
		if (a != null && b == null)
			return true;
		if (a == null && b != null)
			return true;
		if (a != null && !a.equals(b))
			return true;
		return false;
	}

	/**
	 * Get the maximum length supported for an exception's message
	 * parameter string.
	 */

	protected int supportedMessageParamLength() {

		switch (clientType) {

			case JCC_CLIENT:
				return DB2Limit.DB2_JCC_MAX_EXCEPTION_PARAM_LENGTH;
			default:
			// Default is the max for C clients, since that is more
			// restricted than for JCC clients.  Note, though, that
			// JCC clients are the only ones supported right now.
				return DB2Limit.DB2_CCC_MAX_EXCEPTION_PARAM_LENGTH;

		}

	}

	/**
	 * Get the type of the client.
	 */

	protected int getClientType() {

		return clientType;

	}

}
