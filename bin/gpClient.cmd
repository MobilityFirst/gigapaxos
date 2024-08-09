@echo off
setlocal enabledelayedexpansion

set BINFILE=%~f0
if "%BINFILE%"=="" (
  set BINFILE=%0
)


for %%I in ("%BINFILE%") do set BINDIR=%%~dpI

set BINDIR=%BINDIR:~0,-1%

pushd %BINDIR%\..
set HEAD=%CD%
popd


set VERBOSE=1
set FILESET=

for %%F in ("%HEAD%\jars\*.jar" "%HEAD%\jars\*.class") do (
  if exist %%F (
    for %%I in (%%F) do (
      set "FILESET=!FILESET! %%~fI"
    )
  )
)

set FILESET=%FILESET:~1%

set "DEFAULT_GP_CLASSPATH="
for %%F in (%FILESET%) do (
    set "DEFAULT_GP_CLASSPATH=!DEFAULT_GP_CLASSPATH!;%%F"
)
set "DEFAULT_GP_CLASSPATH=!DEFAULT_GP_CLASSPATH:~1!"

set "DEV_MODE=1"
set "ENABLE_ASSERTS="
if "%DEV_MODE%"=="1" (
    set "DEFAULT_GP_CLASSPATH=%HEAD%\build\classes;%HEAD%\build\test\classes;%DEFAULT_GP_CLASSPATH%"
    set "ENABLE_ASSERTS=-ea"
)

set "CLASSPATH=%DEFAULT_GP_CLASSPATH%"
set CONF=conf
set CONFDIR=%HEAD%\%CONF%


set DEFAULT_LOG_PROPERTIES=logging.properties
call :SET_DEFAULT_CONF %DEFAULT_LOG_PROPERTIES%
set LOG_PROPERTIES=%retval%

set DEFAULT_LOG4J_PROPERTIES=log4j.properties
call :SET_DEFAULT_CONF %DEFAULT_LOG4J_PROPERTIES%
set LOG4J_PROPERTIES=%retval%

set DEFAULT_GP_PROPERTIES=gigapaxos.properties
call :SET_DEFAULT_CONF %DEFAULT_GP_PROPERTIES%
set GP_PROPERTIES=%retval%

call :SET_DEFAULT_CONF "keyStore.jks"
set KEYSTORE=%retval%

call :SET_DEFAULT_CONF "trustStore.jks"
set TRUSTSTORE=%retval%

set "ACTIVE=active"
set "RECONFIGURATOR=reconfigurator"

set "SSL_OPTIONS= -Djavax.net.ssl.keyStorePassword=qwerty -Djavax.net.ssl.keyStore=%KEYSTORE% -Djavax.net.ssl.trustStorePassword=qwerty -Djavax.net.ssl.trustStore=%TRUSTSTORE%"

set ARGS_EXCEPT_CLASSPATH=

for %%A in (%*) do (
  set "arg=%%~A"
  if "!arg!"=="-cp" (
    set skip_next=1
  ) else if "!arg!"=="-classpath" (
      set skip_next=1
  ) else if defined skip_next (
      set skip_next=
  ) else (
      set "ARGS_EXCEPT_CLASSPATH=!ARGS_EXCEPT_CLASSPATH! %%A"
  )
)

set "ARGS_EXCEPT_CLASSPATH=!ARGS_EXCEPT_CLASSPATH:~1!"

set CLASSPATH_SUPPLIED=
for %%A in (%*) do (
  set "arg=%%~A"
  if "!arg!"=="-cp" (
    set skip_next=1
  ) else if "%%A"=="-classpath" (
      set skip_next=1
  ) else if defined skip_next (
      set "CLASSPATH_SUPPLIED=!arg!"
  )
)

if not "%CLASSPATH_SUPPLIED%"=="" (
    set CLASSPATH=%CLASSPATH_SUPPLIED%;%CLASSPATH%
)

set DEFAULT_CLIENT_ARGS=
for %%A in (%*) do (
    set "arg=%%~A"
    if not "!arg:~0,2!"=="-D" (
        set "DEFAULT_CLIENT_ARGS=!DEFAULT_CLIENT_ARGS! !arg!"
    )
)

set "DEFAULT_CLIENT_ARGS=!DEFAULT_CLIENT_ARGS:~1!"

for %%A in (%ARGS_EXCEPT_CLASSPATH%) do (
  set "arg=%%~A"
  echo !arg! | findstr /r /c:"^-D.*=" >nul
  if !errorlevel! == 0 (
    for /f "tokens=1,2 delims==" %%i in ("!arg:-D=!") do (
      set "key=%%i"
      set "value=%%j"
    )
    if "!key!" == "gigapaxosConfig" (
        set "GP_PROPERTIES=%HEAD%\!value!"
    )
  )
)

if "%GP_PROPERTIES%"=="" (
  goto error
)

goto end

:error
echo Error: Unable to find file %DEFAULT_GP_PROPERTIES% >&2
exit /b 1

:end

set "ARGS_EXCEPT_CLASSPATH_="
for %%A in (%ARGS_EXCEPT_CLASSPATH%) do (
  set "arg=%%~A"
  set "ARGS_EXCEPT_CLASSPATH_=!ARGS_EXCEPT_CLASSPATH_! !arg!"
)
set "ARGS_EXCEPT_CLASSPATH_=!ARGS_EXCEPT_CLASSPATH_:~1!"
set "DEFAULT_JVMARGS= %ENABLE_ASSERTS% -classpath %CLASSPATH% -Djava.util.logging.config.file=%LOG_PROPERTIES% -Dlog4j.configuration=log4j.properties -DgigapaxosConfig=%GP_PROPERTIES%"

set "JVM_APP_ARGS=%DEFAULT_JVMARGS% !ARGS_EXCEPT_CLASSPATH_!"

set "APP="
for /f "usebackq tokens=*" %%A in ("%GP_PROPERTIES%") do (
  set "line=%%A"
  if not "!line:~0,1!" == "#" (
    echo !line! | findstr /r /c:"^[ \t]*APPLICATION=" >nul
    if not errorlevel 1 (
      for /f "tokens=2 delims==" %%B in ("!line!") do (
        set "APP=%%B"
      )
    )
  )
)
for /f "tokens=* delims= " %%C in ("!APP!") do set "APP=%%C"

if "%APP%"=="" (
  set "APP=edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp"
)

set "DEFAULT_CLIENT="
if "%APP%"=="edu.umass.cs.gigapaxos.examples.noop.NoopPaxosApp" if "%DEFAULT_CLIENT_ARGS%"=="" (
  set "DEFAULT_CLIENT=edu.umass.cs.gigapaxos.examples.noop.NoopPaxosAppClient"
) else if "%APP%"=="edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp" if "%DEFAULT_CLIENT_ARGS%"=="" (
  set "DEFAULT_CLIENT=edu.umass.cs.reconfiguration.examples.NoopAppClient"
)

if "%DEFAULT_CLIENT%"=="" (
    set "showUsage=false"
    if "%~1"=="" (
        set "showUsage=true"
        goto :showUsage
    ) else (
        for %%A in (%*) do (
            set "arg=%%~A"
            if "!argg!"=="-help" (
                set "showUsage=true"
                goto :showUsage
            )
        )
    )

    :showUsage
    if "%showUsage%"=="true" (
        echo "Usage: gpClient.sh [JVMARGS] CLIENT_CLASS_NAME"
        echo "Example: gpClient.sh -cp jars/myclient.jar edu.umass.cs.reconfiguration.examples.NoopAppClient"
        exit /b
    )

)

echo java %SSL_OPTIONS% %JVM_APP_ARGS% %DEFAULT_CLIENT%
java %SSL_OPTIONS% %JVM_APP_ARGS% %DEFAULT_CLIENT%




:SET_DEFAULT_CONF
set default=%~1

if exist %CONFDIR%\%default% (
    set "returnValue=%CONFDIR%\%default%"
) else (
    for %%I in (%CONFDIR%\%default%) do (
        if exist "%%~fI" (
            for /f "tokens=*" %%J in ('powershell -command "(Get-Item -Path %%I).Target"') do (
                set linkTarget=%%J
            )
            if exist "!linkTarget!" (
                set returnValue=!linkTarget!
            )
        )
    )
)
set "retval=%returnValue%"
goto :EOF

endlocal