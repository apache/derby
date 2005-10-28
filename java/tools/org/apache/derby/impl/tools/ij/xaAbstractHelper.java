/*

   Derby - Class org.apache.derby.impl.tools.ij.xaAbstractHelper

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
