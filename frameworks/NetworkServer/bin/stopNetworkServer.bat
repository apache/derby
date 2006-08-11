
@REM Licensed to the Apache Software Foundation (ASF) under one or more
@REM contributor license agreements.  See the NOTICE file distributed with
@REM this work for additional information regarding copyright ownership.
@REM The ASF licenses this file to you under the Apache License, Version 2.0
@REM (the "License"); you may not use this file except in compliance with
@REM the License.  You may obtain a copy of the License at
@REM
@REM     http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing, software
@REM distributed under the License is distributed on an "AS IS" BASIS,
@REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM See the License for the specific language governing permissions and
@REM limitations under the License.

@REM --
@REM -- REQUIREMENTS:
@REM --         You must have the derby jar files as well as the
@REM --   derby network server class files in your classpath.
@REM --
@REM --  Check the setNetworkServerCP.bat file for an example of
@REM --   what to set.
@REM -- This file for use on Windows systems
@REM ---------------------------------------------------------

@echo off
rem set DERBY_INSTALL=

@if "%DERBY_HOME%"=="" set DERBY_HOME=%DERBY_INSTALL%
@if "%DERBY_HOME%"=="" goto noderbyhome

@if "%JAVA_HOME%"=="" goto nojavahome
@if not exist "%JAVA_HOME%\bin\java.exe" goto nojavahome

:set_host

if "%1" == "" goto setServerHost
set derbyHost=%1
goto set_port

:setServerHost
if not "%DERBY_SERVER_HOST%" == "" goto setServerHost2
set derbyHost=localhost
goto set_port

:setServerHost2
set derbyHost=%DERBY_SERVER_HOST%


:set_port
shift
if "%1" == "" goto setServerPort
set derbyPort=%1
goto stop_server


:setServerPort
if not "%DERBY_SERVER_PORT%" == ""  goto setServerPort2
set derbyPort=1527
goto stop_server

:setServerPort2
set derbyPort=%DERBY_SERVER_PORT%

:stop_server

@REM ---------------------------------------------------------
@REM -- shutdown Derby as a Network server
@REM ---------------------------------------------------------
%JAVA_HOME%\bin\java -cp "%DERBY_HOME%\lib\derby.jar;%DERBY_HOME%\lib\derbytools.jar;%DERBY_HOME%\lib\derbynet.jar;%CLASSPATH%" org.apache.derby.drda.NetworkServerControl shutdown -h %derbyHost% -p %derbyPort%

goto end
@REM ---------------------------------------------------------
@REM -- To use a different JVM with a different syntax, simply edit
@REM -- this file
@REM ---------------------------------------------------------

:nojavahome
echo JAVA_HOME not set or could not find java executable in JAVA_HOME.
echo Please set JAVA_HOME to the location of a valid Java installation.
goto end

:noderbyhome
echo DERBY_HOME or DERBY_INSTALL not set. Set one of these variables
echo to the location of your Derby installation.
goto end

:end
set derbyPort=
set derbyHost=
