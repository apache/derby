/*

	Derby - Class org.apache.derby.ui.common.Messages
	
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


package org.apache.derby.ui.common;

public class Messages {
	public static String D_NS_ATTEMPT_STARTED="Attempting to start the Apache Derby Network Server on port ";
	public static String D_NS_ATTEMPT_STOPPED="Attempting to stop the Apache Derby Network Server on port ";
	
	//public static String D_NS_STARTED="Apache Derby Network Server started.";
	//public static String D_NS_STOPPED="Apache Derby Network Server stopped.";

	public static String D_NS_START_ERROR="Error starting Derby Network Server:\n";
	public static String D_NS_STOP_ERROR="Error stopping Derby Network Server:\n";
	
	public static String ADDING_NATURE="Adding Apache Derby Nature...";
	public static String DERBY_NATURE_ADDED="Finished adding Apache Derby Nature to the selected project";
	public static String ERROR_ADDING_NATURE="Error adding Derby jars to the project";
	
	public static String REMOVING_NATURE="Removing Apache Derby Nature...";
	public static String DERBY_NATURE_REMOVED="Finished removing Apache Derby Nature from the selected project";
	public static String ERROR_REMOVING_NATURE="Error removing Derby jars from the project";
	
	public static String NO_DERBY_NATURE="The selected project does not have an Apache Derby nature.";
	public static String ADD_N_TRY="Please add the Derby nature and try again.";
	
	public static String NO_ACTION="Unable to execute the action";
	public static String SERVER_RUNNING="The Network Server is already running.\nStop the server prior to changing the settings.";
	
	public static String DERBY_CONTAINER_DESC = "Derby Libraries";
}
