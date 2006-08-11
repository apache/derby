
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
@REM -- This batch file sets the CLASSPATH environment variable
@REM -- for use with Derby products in embedded mode
@REM --
@REM -- To use this script from other locations, change the 
@REM -- value assigned to DERBY_INSTALL to be an absolute path 
@REM -- (set DERBY_INSTALL=C:\derby) instead of the current relative path
@REM --
@REM -- This file for use on Windows systems
@REM -- 
@REM ---------------------------------------------------------
@echo off
rem set DERBY_INSTALL=

@if "%DERBY_HOME%"=="" set DERBY_HOME=%DERBY_INSTALL%
@if "%DERBY_HOME%"=="" goto noderbyhome

@FOR %%X in ("%DERBY_HOME%") DO SET DERBY_HOME=%%~sX

set CLASSPATH=%DERBY_HOME%\lib\derby.jar;%DERBY_HOME%\lib\derbytools.jar;%CLASSPATH%
@goto end

:noderbyhome
@echo DERBY_HOME or DERBY_INSTALL not set. Set one of these variables
@echo to the location of your Derby installation.
@goto end

:end