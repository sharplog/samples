@echo off

set workpath=D:\Tools\binggetter

set "today=%date:~0,4%%date:~5,2%%date:~8,2%"

for /f %%i in (%workpath%\date.txt) do ( set "lastdate=%%i" )

if %lastdate% lss %today% (
	echo %today% > %workpath%\date.txt
	java -jar %workpath%\binggetter.jar > %workpath%\log.txt
)
