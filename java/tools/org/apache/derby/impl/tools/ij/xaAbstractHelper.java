/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.tools.ij
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.tools.ij;


import java.sql.SQLException;
import java.sql.Connection;

/*
	An interface for running xa tests.
	The real implementation is only loaded if the requisite javax classes are
	in the classpath. 
 */
interface xaAbstractHelper
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;

	void XADataSourceStatement(ij parser, Token dbname, Token shut, String create) throws SQLException;
	void XAConnectStatement(ij parser, Token user, Token pass, String id) throws SQLException;
	void XADisconnectStatement(ij parser, String n) throws SQLException;
	Connection XAGetConnectionStatement(ij parser, String n) throws SQLException;
	void CommitStatement(ij parser, Token onePhase, Token twoPhase, int xid) throws SQLException;
	void EndStatement(ij parser, int flag, int xid) throws SQLException;
	void ForgetStatement(ij parser, int xid) throws SQLException;
	void PrepareStatement(ij parser, int xid) throws SQLException;
	ijResult RecoverStatement(ij parser, int flag) throws SQLException;
	void RollbackStatement(ij parser, int xid) throws SQLException;
	void StartStatement(ij parser, int flag, int xid) throws SQLException;
	Connection DataSourceStatement(ij parser, Token dbname, Token protocol,
								   Token userT, Token passT, String id) throws SQLException;
	void CPDataSourceStatement(ij parser, Token dbname, Token protocol) throws SQLException;
	void CPConnectStatement(ij parser, Token userT, Token passT, String n) throws SQLException;
	Connection CPGetConnectionStatement(ij parser, String n) throws SQLException;
	void CPDisconnectStatement(ij parser, String n) throws SQLException;
	void setFramework(String framework);

}
