
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

@REM ---------------------------------------------------------
@REM -- This batch file is an example of how to start dblook in 
@REM -- an NetworkServer environment.
@REM --
@REM -- REQUIREMENTS: 
@REM -- You must have the Derby libraries in your classpath
@REM -- 
@REM -- See the setNetworkClientCP.bat for an example of
@REM -- how to do this.
@REM --
@REM -- This file for use on Windows systems
@REM ---------------------------------------------------------
@echo off
rem set DERBY_INSTALL=

@if "%DERBY_HOME%"=="" set DERBY_HOME=%DERBY_INSTALL%
@if "%DERBY_HOME%"=="" goto noderbyhome

@if "%JAVA_HOME%"=="" goto nojavahome
@if not exist "%JAVA_HOME%\bin\java.exe" goto nojavahome

@REM ---------------------------------------------------------
@REM -- start dblook
@REM ---------------------------------------------------------
"%JAVA_HOME%\bin\java" -cp "%DERBY_HOME%\lib\derbyclient.jar;%DERBY_HOME%\lib\derbytools.jar;%CLASSPATH%" org.apache.derby.tools.dblook %*
@GOTO end

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