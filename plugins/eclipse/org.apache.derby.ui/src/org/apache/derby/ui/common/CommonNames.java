/*

	Derby - Class org.apache.derby.ui.common.CommonNames
	
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


public class CommonNames {
	//Network Server related
	public static String DERBY_SERVER_CLASS="org.apache.derby.drda.NetworkServerControl";
	public static String DERBY_SERVER="Derby Network Server";
	public static String START_DERBY_SERVER="start";
	public static String SHUTDOWN_DERBY_SERVER="shutdown";
	
	//Tools related
	public static String SYSINFO_CLASS="org.apache.derby.tools.sysinfo";
	public static String SYSINFO="SYSINFO";
	public static String IJ_CLASS="org.apache.derby.tools.ij";
	public static String IJ="IJ";
	public static String SQL_SCRIPT="SQL Script";
	
	//actual information
	public static String CORE_PATH="org.apache.derby.core";
	public static String UI_PATH="org.apache.derby.ui";
	public static String PLUGIN_NAME="Apache Derby Ui Plug-in";
	
	//The next to be used with UI_PATH for adding nature. isrunning and decorator
	public static String DERBY_NATURE=UI_PATH+"."+"derbyEngine";
	public static String ISRUNNING="isrun";
	public static String RUNDECORATOR=UI_PATH+"."+"DerbyIsRunningDecorator";
	
	//Launch Config Types
	public static String START_SERVER_LAUNCH_CONFIG_TYPE=UI_PATH+".startDerbyServerLaunchConfigurationType";
	public static String STOP_SERVER_LAUNCH_CONFIG_TYPE=UI_PATH+".stopDerbyServerLaunchConfigurationType";
	public static String IJ_LAUNCH_CONFIG_TYPE=UI_PATH+".ijDerbyLaunchConfigurationType";
	public static String SYSINFO_LAUNCH_CONFIG_TYPE=UI_PATH+".sysinfoDerbyLaunchConfigurationType";
	
	//JVM Poperties
	public static String D_IJ_PROTOCOL=" -Dij.protocol=";
	public static String DERBY_PROTOCOL="jdbc:derby:";
	public static String D_SYSTEM_HOME=" -Dderby.system.home=";
	
}
