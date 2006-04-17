@REM -- This file for use on Windows systems
@REM ---------------------------------------------------------

@echo off
@rem set DERBY_INSTALL=

@if "%DERBY_HOME%"=="" set DERBY_HOME=%DERBY_INSTALL%
@if "%DERBY_HOME%"=="" goto noderbyhome

@if "%JAVA_HOME%"=="" goto nojavahome
@if not exist "%JAVA_HOME%\bin\java.exe" goto nojavahome

@if !"%CLASSPATH%"==! call "%DERBY_HOME%"/frameworks/NetworkServer/bin/setNetworkServerCP.bat
@if "%CLASSPATH%" == "" call "%DERBY_HOME%"/frameworks/NetworkServer/bin/setNetworkServerCP.bat

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
goto start_server


:setServerPort
if not "%DERBY_SERVER_PORT%" == ""  goto setServerPort2
set derbyPort=1527
goto start_server

:setServerPort2
set derbyPort=%DERBY_SERVER_PORT%

:start_server

@REM ---------------------------------------------------------
@REM -- start Derby as a Network server
@REM ---------------------------------------------------------
"%JAVA_HOME%\bin\java" org.apache.derby.drda.NetworkServerControl start -h %derbyHost% -p %derbyPort%

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
