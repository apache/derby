@echo off
@REM Licensed to the Apache Software Foundation (ASF) under one
@REM or more contributor license agreements.  See the NOTICE file
@REM distributed with this work for additional information
@REM regarding copyright ownership.  The ASF licenses this file
@REM to you under the Apache License, Version 2.0 (the
@REM "License"); you may not use this file except in compliance
@REM with the License.  You may obtain a copy of the License at

@REM   http://www.apache.org/licenses/LICENSE-2.0

@REM Unless required by applicable law or agreed to in writing,
@REM software distributed under the License is distributed on an
@REM "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@REM KIND, either express or implied.  See the License for the
@REM specific language governing permissions and limitations
@REM under the License.

rem %~dp0 is expanded pathname of the current script under NT
set SRCDIR=%~dp0..\..
set THISDIR=%~dp0
for %%A in ("%SRCDIR%\java\shared") do set SHAREDDIR=%%~dpfA
for %%A in ("%SRCDIR%\java\engine") do set ENGINEDIR=%%~dpfA
for %%A in ("%SRCDIR%\java\client") do set CLIENTDIR=%%~dpfA

rem check the value of DERBY_HOME
if exist "%DERBY_HOME%\lib\derby.jar" goto setLocalClassPath

:noDerbyHome
echo DERBY_HOME is set incorrectly or derby.jar could not be located. Please set DERBY_HOME.
goto end

:setLocalClasspath
set LOCALCLASSPATH=%DERBY_HOME%\lib\derby.jar;%DERBY_HOME%\lib\derbyclient.jar
@rmdir /s /q %DERBY_HOME%\jdbc4classes

:checkJava
set _JAVACMD=%JAVACMD%

if "%JAVA_HOME%" == "" goto noJavaHome
if not exist "%JAVA_HOME%\bin\java.exe" goto noJavaHome
if "%_JAVACMD%" == "" set _JAVACMD=%JAVA_HOME%\bin\javac.exe
if "%_JARCMD%" == "" set _JARCMD=%JAVA_HOME%\bin\jar.exe
goto endcheck

:noJavaHome
if "%_JAVACMD%" == "" set _JAVACMD=javac.exe
if "%_JARCMD%" == "" set _JAVACMD=jar.exe

:endcheck
echo Building Derby client classes for JDBC 4.0...
mkdir %DERBY_HOME%\jdbc4classes\client
cd %SRCDIR%\java\client
FOR /F %%G in (%THISDIR%client.list) do %_JAVACMD% -d %DERBY_HOME%\jdbc4classes\client -classpath %LOCALCLASSPATH% -sourcepath "%CLIENTDIR%;%ENGINEDIR%;%SHAREDDIR%" %%G

echo Updating %DERBY_HOME%\lib\derbyclient.jar
%_JARCMD% uf %DERBY_HOME%\lib\derbyclient.jar -C %DERBY_HOME%\jdbc4classes\client org

echo Building Derby engine classes for JDBC 4.0
mkdir %DERBY_HOME%\jdbc4classes\engine
@rem stop the compiler from trying to recompile EmbedDatabaseMetaData
cd %DERBY_HOME%\jdbc4classes\engine
%_JARCMD% xf %DERBY_HOME%\lib\derby.jar org\apache\derby\impl\jdbc\EmbedDatabaseMetaData.class
%_JARCMD% xf %DERBY_HOME%\lib\derby.jar org\apache\derby\modules.properties
cd %SRCDIR%\java\engine
FOR /F %%G in (%THISDIR%engine.list) do %_JAVACMD% -d %DERBY_HOME%\jdbc4classes\engine -classpath %LOCALCLASSPATH% -sourcepath "%ENGINEDIR%;%SHAREDDIR%" %%G
copy /b %DERBY_HOME%\jdbc4classes\engine\org\apache\derby\modules.properties+%THISDIR%\modules.patch %DERBY_HOME%\jdbc4classes\engine\org\apache\derby\modules.properties
del /q %DERBY_HOME%\jdbc4classes\engine\org\apache\derby\impl\jdbc\EmbedDatabaseMetaData.class

echo Updating %DERBY_HOME%\lib\derby.jar
%_JARCMD% uf %DERBY_HOME%\lib\derby.jar -C %DERBY_HOME%\jdbc4classes\engine org

cd %THISDIR%

echo Cleaning up
@rmdir /s /q %DERBY_HOME%\jdbc4classes
echo Done.
:end
