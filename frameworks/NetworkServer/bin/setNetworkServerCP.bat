@REM ---------------------------------------------------------
@REM -- This batch file sets the CLASSPATH environment variable
@REM -- for use with Derby products in NetworkServer mode
@REM --
@REM -- To use this script from other locations, change the 
@REM -- value assigned to CLOUDSCAPE_INSTALL to be an absolute path 
@REM -- (set CLOUDSCAPE_INSTALL=C:\derby) instead of the current relative path
@REM --
@REM -- This file for use on Windows systems
@REM -- 
@REM ---------------------------------------------------------

rem set CLOUDSCAPE_INSTALL=

FOR %%X in ("%CLOUDSCAPE_INSTALL%") DO SET CLOUDSCAPE_INSTALL=%%~sX

set CLASSPATH=%CLOUDSCAPE_INSTALL%\lib\derby.jar;%CLOUDSCAPE_INSTALL%\lib\derbytools.jar;%CLOUDSCAPE_INSTALL%\lib\derbynet.jar;%CLASSPATH%
