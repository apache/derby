/*

   Derby - Class org.apache.derby.impl.tools.ij.ijException

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

/**
	ijException is used to get messages from the ij parser to
	the main ij loop. Because this is not under the protocol/impl
	umbrella, it does not have available to it the message service.
	At this time, all messages are hard-coded in this file. A more
	serviceable solution may need to be found.

	@author ames.
 */

public class ijException extends RuntimeException {

	private	final static String	IllegalStatementName="IJ_IllegalStatementName";
	private final static String NotYetImplemented="IJ_NotYetImpl";
	private final static String AlreadyHaveConnectionNamed = "IJ_AlreHaveACon";
	private final static String BangException = "IJ_ExceRunnComm";
	private final static String ConnectionGetWarningsFailed = "IJ_UnabToGetWar";
	private final static String ClassNotFoundForProtocol = "IJ_CoulNotLocaC";
	private final static String ClassNotFound = "IJ_CoulNotLocaC_5";
	private final static String DisconnectFailed = "IJ_FailToDisc";
	private final static String DriverNotClassName = "IJ_DrivNotClasN";
	private final static String FileNotFound = "IJ_FileNotFoun";
	private final static String ForwardOnlyCursor = "IJ_IsNotAlloOnA";
	private final static String GetConnectionFailed = "IJ_GetcCallFail";
	private final static String IOException = "IJ_Ioex";
	private final static String NeedToDisconnect = "IJ_NeedToDiscFi";
	private final static String NoSuchAsyncStatement = "IJ_NoAsynStatEx";
	private final static String NoSuchConnection = "IJ_NoConnExisWi";
	private final static String NoSuchProtocol = "IJ_NoProtExisWi";
	private final static String NotJDBC20 = "IJ_IsOnlySuppIn";
	private final static String NoUsingResults = "IJ_UsinClauHadN";
	private final static String ObjectWasNull = "IJ_UnabToEsta";
	private final static String ResultSetGetWarningsFailed = "IJ_UnabToGetWar_19";
    private final static String ResourceNotFound = "IJ_ResoNotFoun";
	private final static String ScrollCursorsNotSupported = "IJ_ScroCursAre1";
	private final static String HoldCursorsNotSupported = "IJ_HoldCursAre4";
	private final static String StatementGetWarningsFailed = "IJ_UnabToGetWar_22";
	private final static String WaitInterrupted = "IJ_WaitForStatI";
	private final static String ZeroInvalidForAbsolute = "IJ_0IsAnInvaVal";

	public ijException(String message) {
		super(message);
	}

	static ijException notYetImplemented() {
		return new ijException(LocalizedResource.getMessage(NotYetImplemented));
	}

	static ijException illegalStatementName(String n) {
		return new ijException(LocalizedResource.getMessage(IllegalStatementName, n));
	}
	static ijException alreadyHaveConnectionNamed(String n) {
		return new ijException(LocalizedResource.getMessage(AlreadyHaveConnectionNamed, n));
	}
	static ijException bangException(Throwable t) {
		return new ijException(LocalizedResource.getMessage(BangException, t.toString()));
	}
	static ijException classNotFoundForProtocol(String p) {
		return new ijException(LocalizedResource.getMessage(ClassNotFoundForProtocol, p));
	}
	static ijException classNotFound(String c) {
		return new ijException(LocalizedResource.getMessage(ClassNotFound, c));
	}
	static ijException connectionGetWarningsFailed() {
		return new ijException(LocalizedResource.getMessage(ConnectionGetWarningsFailed));
	}
	static ijException disconnectFailed() {
		return new ijException(LocalizedResource.getMessage(DisconnectFailed));
	}
	static ijException driverNotClassName(String c) {
		return new ijException(LocalizedResource.getMessage(DriverNotClassName, c));
	}
	static ijException fileNotFound() {
		return new ijException(LocalizedResource.getMessage(FileNotFound));
	}
	static public ijException forwardOnlyCursor(String operation) {
		return new ijException(LocalizedResource.getMessage(ForwardOnlyCursor, operation));
	}
	static ijException resourceNotFound() {
		return new ijException(LocalizedResource.getMessage(ResourceNotFound));
	}
	static ijException getConnectionFailed() {
		return new ijException(LocalizedResource.getMessage(GetConnectionFailed));
	}
	static ijException iOException(IOException t) {
		return new ijException(LocalizedResource.getMessage(IOException, t.getMessage()));
	}
	static ijException needToDisconnect() {
		return new ijException(LocalizedResource.getMessage(NeedToDisconnect));
	}
	static ijException noSuchAsyncStatement(String c) {
		return new ijException(LocalizedResource.getMessage(NoSuchAsyncStatement, c));
	}
	static ijException noSuchConnection(String c) {
		return new ijException(LocalizedResource.getMessage(NoSuchConnection, c));
	}
	static ijException noSuchProtocol(String c) {
		return new ijException(LocalizedResource.getMessage(NoSuchProtocol, c));
	}
	static public ijException notJDBC20(String operation) {
		return new ijException(LocalizedResource.getMessage(NotJDBC20, operation));
	}
	static ijException noUsingResults() {
		return new ijException(LocalizedResource.getMessage(NoUsingResults));
	}
	static public ijException objectWasNull(String objectName) {
		return new ijException(LocalizedResource.getMessage(ObjectWasNull, objectName));
	}
	static ijException resultSetGetWarningsFailed() {
		return new ijException(LocalizedResource.getMessage(ResultSetGetWarningsFailed));
	}
	static ijException scrollCursorsNotSupported() {
		return new ijException(LocalizedResource.getMessage(ScrollCursorsNotSupported));
	}
	//IJImpl20.utilMain can't throw exception for holdable cursors if
	//following not declared public
	public static ijException holdCursorsNotSupported() {
		return new ijException(LocalizedResource.getMessage(HoldCursorsNotSupported));
	}
	static ijException statementGetWarningsFailed() {
		return new ijException(LocalizedResource.getMessage(StatementGetWarningsFailed));
	}
	static ijException waitInterrupted(Throwable t) {
		return new ijException(LocalizedResource.getMessage(WaitInterrupted, t.toString()));
	}
	public static ijException zeroInvalidForAbsolute() {
		return new ijException(LocalizedResource.getMessage(ZeroInvalidForAbsolute));
	}
}
