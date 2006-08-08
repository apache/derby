/*

   Derby - Class org.apache.derbyTesting.unitTests.services.T_StandardException

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

import org.apache.derby.iapi.error.StandardException;

import java.text.MessageFormat;
/**
  A standard exception for testing.

  The messages for this exception are not localized or stored
  with the product.
 */
public class T_StandardException extends StandardException
{
	String msgText = "Message text not set";

	protected T_StandardException(String messageID, String msgText)
	{
		super(messageID);
		myConstructorCommon( messageID, msgText );
	}
	protected T_StandardException(String messageID, String msgText, Throwable t)
	{
		super(messageID, t, (Object[]) null);
		myConstructorCommon( messageID, msgText );
	}
	protected T_StandardException(String messageID, String msgText, Throwable t, Object[] args)
	{
		super(messageID, t, args);
		myConstructorCommon( messageID, msgText );
	}

	protected	void	myConstructorCommon( String messageID, String msgText )
	{
		this.msgText = msgText;
	}

	public static
	StandardException newT_StandardException(String messageID, Throwable t, String msgText)
	{
		return new T_StandardException(messageID,msgText,t);
	}

	public static
	StandardException newT_StandardException(String messageID, String msgText)
	{
		return new T_StandardException(messageID,msgText);
	}

	public String getMessage() {return MessageFormat.format(msgText, getArguments());}
	public String getErrorProperty() {throw new Error("method not supported");}
}
