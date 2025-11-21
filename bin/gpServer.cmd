@echo off

setlocal enabledelayedexpansion
set "APP_ARGS_KEY=appArgs"
set "APP_RESOURCES_KEY=appResourcePath"
set "DEBUG_KEY=debug"

set "keywords=start stop restart clear forceclear"
set "found="

if "%~1"=="" (
    set "found="
) else (
    for %%i in (%*) do (
        for %%j in (%keywords%) do (
            if "%%i"=="%%j" (
                set found=1
            )
        )
    )
)
set scriptDir=%~dp0
for %%a in ("%scriptDir:~0,-1%") do (
    set parentDir=%%~nxa
)

set "SCRIPT_FILENAME=%parentDir%\%~nx0"
if not defined found (
    echo Usage: %SCRIPT_FILENAME% [JVMARGS] [-D%APP_RESOURCES_KEY%=APP_RESOURCES_DIR] [-D%APP_ARGS_KEY%="APP_ARGS"] [-%DEBUG_KEY%] ^stop^|^start^|^restart^|^clear^|^forceclear all^|server_names
    echo Examples:
    echo     .\%SCRIPT_FILENAME% start AR1
    echo     .\%SCRIPT_FILENAME% start AR1 AR2 RC1
    echo     .\%SCRIPT_FILENAME% start all
    echo     .\%SCRIPT_FILENAME% stop AR1 RC1
    echo     .\%SCRIPT_FILENAME% stop all
    echo     .%SCRIPT_FILENAME% "-DgigapaxosConfig=\path\to\gigapaxos.properties" start all
    echo     .\%SCRIPT_FILENAME% -cp myjars1.jar;myjars2.jar "-DgigapaxosConfig=\path\to\gigapaxos.properties" "-D%APP_RESOURCES_KEY%=\path\to\app\resources\dir\" "-D%APP_ARGS_KEY%=""-opt1=val1 -flag2 -str3=\""quoted arg example\"" -n 50"" " -%DEBUG_KEY% start all
    echo  Note: -%DEBUG_KEY% option is insecure and should only be used during testing and development.
    exit /b 0
)

set BINFILE=%~f0
if "%BINFILE%"=="" (
  set BINFILE=%0
)


for %%I in ("%BINFILE%") do set BINDIR=%%~dpI

set BINDIR=%BINDIR:~0,-1%

call "%BINDIR%"\gpEnv.cmd

pushd %BINDIR%\..
set HEAD=%CD%
popd

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
    if exist %HEAD%\build\classes (
        set "DEFAULT_GP_CLASSPATH=%HEAD%\build\classes;%DEFAULT_GP_CLASSPATH%"
    )
    if exist %HEAD%\build\test\classes (
        set "DEFAULT_GP_CLASSPATH=%HEAD%\build\test\classes;%DEFAULT_GP_CLASSPATH%"
    )
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

set "APP_RESOURCES="
set "APP_ARGS="

set "DEFAULT_KEYSTORE=keyStore.jks"
call :SET_DEFAULT_CONF %DEFAULT_KEYSTORE%
set KEYSTORE=%retval%

set "DEFAULT_TRUSTSTORE=trustStore.jks"
call :SET_DEFAULT_CONF %DEFAULT_TRUSTSTORE%
set TRUSTSTORE=%retval%

set VERBOSE=2
set "JAVA=java"
set "ACTIVE_KEYWORD=active"
set "RECONFIGURATOR_KEYWORD=reconfigurator"
set "DEFAULT_APP_RESOURCES=app_resources"
set "DEFAULT_KEYSTORE_PASSWORD=qwerty"
set "DEFAULT_TRUSTSTORE_PASSWORD=qwerty"
set DEBUG_MODE=false
set DEBUG_PORT=10000

set "ARGS_EXCEPT_CLASSPATH_DEBUG="

for %%A in (%*) do (
    set "arg=%%~A"
    if not "!arg!"=="-%DEBUG_KEY%" (
        if "!arg!"=="-cp" (
            set skip_next=1
        ) else if "!arg!"=="-classpath" (
            set skip_next=1
        ) else if defined skip_next (
            set skip_next=
        ) else (
            set "ARGS_EXCEPT_CLASSPATH_DEBUG=!ARGS_EXCEPT_CLASSPATH_DEBUG! %%A"
        )
    )
)
set "ARGS_EXCEPT_CLASSPATH_DEBUG=!ARGS_EXCEPT_CLASSPATH_DEBUG:~1!"
set "temp_args="
for %%A in (%ARGS_EXCEPT_CLASSPATH_DEBUG%) do (
    set "arg=%%A"
    if "%arg%"=="-%APP_ARGS_KEY%" (
        goto: skip_remaining_args
    ) else (
        set "temp_args=!temp_args! %%A"
    )
)
:skip_remaining_args

set "SUPPLIED_JVMARGS="
for %%A in (%temp_args%) do (
    set "arg=%%~A"
    if "!arg!"=="start" set "res=T"
    if "!arg!"=="stop" set "res=T"
    if "!arg!"=="restart" set "res=T"
    if "!arg!"=="clear" set "res=T"
    if "!arg!"=="forceclear" set "res=T"

    if "!res!"=="T" (
        goto :done
    ) else (
        set "SUPPLIED_JVMARGS=!SUPPLIED_JVMARGS! %%A"
    )
)
:done

set SUPPLIED_JVMARGS=%SUPPLIED_JVMARGS:~1%

set "ARG_CLASSPATH="
for %%A in (%*) do (
  if "%%A"=="-cp" (
    set skip_next=1
  ) else if "%%A"=="-classpath" (
      set skip_next=1
  ) else if defined skip_next (
      set ARG_CLASSPATH=%%A
  )
)

if not "%ARG_CLASSPATH%"=="" (
    set CLASSPATH=%ARG_CLASSPATH%;%CLASSPATH%
)

set index=1
set "args="
for %%A in (%*) do (
    set "arg=%%~A"
    echo !arg! | findstr /r /c:"^-D.*=" >nul
    if !errorlevel! == 0 (
        for /f "tokens=1,2 delims==" %%i in ("!arg:-D=!") do (
            set "key=%%i"
            set "value=%%j"
        )
        if "!key!" == "gigapaxosConfig" (
            set "GP_PROPERTIES=%HEAD%\!value!"
        ) else if "!key!" == "%APP_RESOURCES_KEY%" (
            set "APP_RESOURCES=!value!"
        ) else if "!key!" == "%APP_ARGS_KEY%" (
            set "APP_ARGS=!value!"
        )
    ) else if "!arg!"=="-%DEBUG_KEY%" (
        set DEBUG_MODE=true
    ) else (
        set "res="
        if "!arg!"=="start" set "res=T"
        if "!arg!"=="stop" set "res=T"
        if "!arg!"=="restart" set "res=T"
        if "!arg!"=="clear" set "res=T"
        if "!arg!"=="forceclear" set "res=T"

        if "!res!"=="T" (
            set i=1
            for %%A in (%*) do (
                if !i! geq !index! (
                    set "args=!args! %%A"
                )
                set /A i+=1
            )
            set args=!args:~1!
        )
    )
    set /A index+=1
)
if "%GP_PROPERTIES%"=="" (
  goto error
)

goto end

:error
echo Error: Unable to find file %DEFAULT_GP_PROPERTIES% >&2
exit /b 1

:end

if "!APP_RESOURCES:~-1!"=="\" set "APP_RESOURCES=!APP_RESOURCES:~0,-1!"
for %%I in ("%APP_RESOURCES%") do set "APP_RESOURCES_SIMPLE=%%~nxI"

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

for %%I in (%APP:.= %) do set "INSTALL_PATH=%%~nxI"


set curr_ind=0
for %%A in (%args%) do (
    set /A curr_ind+=1
    set "arg=%%~A"
    if !curr_ind! equ 1 (
        set "arg_pos_1=!arg!"\
    )
    if !curr_ind! equ 2 (
        set "arg_pos_2=!arg!"
        goto :found_arg
    )
)
:found_arg

set "reconfigurators="
set "actives="
set "servers="
if "%arg_pos_2%"=="all" (
    for /f "usebackq tokens=*" %%A in ("%GP_PROPERTIES%") do (
        set "line=%%A"
        if "!line:~0,1!" neq "#" (
            echo !line! | findstr /r /c:"^[ \t]*%RECONFIGURATOR_KEYWORD%" >nul
            if not errorlevel 1 (
                set "line2=!line:~15!"
                for /f "delims==" %%B in ("!line2!") do (
                    set "reconfigurator=%%B"
                    set "reconfigurators=!reconfigurators! !reconfigurator!"
                )
            )
            echo !line! | findstr /r /c:"^[ \t]*%ACTIVE_KEYWORD%" >nul
            if not errorlevel 1 (
                set "line3=!line:~7!"
                for /f "delims==" %%B in ("!line3!") do (
                    set "active=%%B"
                    set "actives=!actives! !active!"
                )
            )
        )
    )
    set "actives=!actives:~1!"
    set "servers=!actives!!reconfigurators!"
) else (
    set curr_ind=0
    for %%A in (%args%) do (
        set /A curr_ind+=1
        set "arg=%%A"
        if !curr_ind! geq 2 (
            set "servers=!servers! !arg!"
        )
    )
    set "servers=!servers:~1!"
)
echo servers=[%servers%]

call :get_file_list %*
call :trim_file_list "%conf_transferrables%"

set "conf_transferrables_cygwin="
for %%A in (%conf_transferrables%) do (
    set "currFile=%%~A"
    set "win_path=!currFile:\=/!"
    if "!win_path:~1,1!"==":" (
        set "drive=!win_path:~0,1!"
        set "path_rest=!win_path:~2!"
        set "cyg_path=/cygdrive/!drive!!path_rest!"
    ) else (
        set "cyg_path=!win_path!"
    )
    set "conf_transferrables_cygwin=!conf_transferrables_cygwin! !cyg_path!"
)
set "conf_transferrables_cygwin=%conf_transferrables_cygwin:~1%"



set "SSH=ssh -x -o StrictHostKeyChecking=no -i %SSH_KEY%"
set "RSYNC_PATH=mkdir -p %INSTALL_PATH% %INSTALL_PATH%/%CONF% %INSTALL_PATH%/jars"
set "RSYNC=""%RSYNC_COMMAND%"" --force -aL"
set "RSYNC_SSH_INFO='%SSH_LOC%' -x -o StrictHostKeyChecking=no -i %SSH_KEY%"

set "username="
for /f "usebackq tokens=*" %%A in ("%GP_PROPERTIES%") do (
    set "line=%%A"
    if "!line:~0,1!" neq "#" (
        echo !line! | findstr /r /c:"^[ \t]*USERNAME=" >nul
        if not errorlevel 1 (
            for /f "tokens=2 delims==" %%B in ("!line!") do (
                set "username=%%B"
            )
        )
    )
)
if "%username%"=="" (
    set "username=whoami"
)

set "LINK_CMD="
call :append_to_ln_cmd %GP_PROPERTIES% %DEFAULT_GP_PROPERTIES%
call :append_to_ln_cmd %KEYSTORE% %DEFAULT_KEYSTORE%
call :append_to_ln_cmd %TRUSTSTORE% %DEFAULT_TRUSTSTORE%
call :append_to_ln_cmd %LOG_PROPERTIES% %DEFAULT_LOG_PROPERTIES%
call :append_to_ln_cmd %LOG4J_PROPERTIES% %DEFAULT_LOG4J_PROPERTIES%
call :append_to_ln_cmd %APP_RESOURCES% %DEFAULT_APP_RESOURCES% true


set "LOCAL_SSL_KEYFILES=-Djavax.net.ssl.keyStore=%KEYSTORE% -Djavax.net.ssl.trustStore=%TRUSTSTORE%"
set keyStoreSimpleName=
call :get_simple_name %KEYSTORE%
set keyStoreSimpleName=%res%
set trustStoreSimpleName=
call :get_simple_name %TRUSTSTORE%
set trustStoreSimpleName=%res%
set "REMOTE_SSL_KEYFILES=-Djavax.net.ssl.keyStore=%keyStoreSimpleName% -Djavax.net.ssl.trustStore=%trustStoreSimpleName%"

set "COMMON_JVMARGS=%ENABLE_ASSERTS% -Djavax.net.ssl.keyStorePassword=%keyStorePassword% -Djavax.net.ssl.trustStorePassword=%trustStorePassword%"

set "DEFAULT_JVMARGS=-cp %CLASSPATH% %COMMON_JVMARGS% %LOCAL_SSL_KEYFILES% -Djava.util.logging.config.file=%LOG_PROPERTIES% -Dlog4j.configuration=%LOG4J_PROPERTIES% -DgigapaxosConfig=%GP_PROPERTIES%"

set "JVMARGS=%DEFAULT_JVMARGS% %SUPPLIED_JVMARGS%"

set logPropSimpleName=
call :get_simple_name %LOG_PROPERTIES%
set logPropSimpleName=%res%
set log4jPropSimpleName=
call :get_simple_name %LOG4J_PROPERTIES%
set log4jPropSimpleName=%res%
set defGpSimpleName=
call :get_simple_name %DEFAULT_GP_PROPERTIES%
set defGpSimpleName=%res%

set "DEFAULT_REMOTE_JVMARGS=%COMMON_JVMARGS% %REMOTE_SSL_KEYFILES% -Djava.util.logging.config.file=%logPropSimpleName% -Dlog4j.configuration=%log4jPropSimpleName% -DgigapaxosConfig=%defGpSimpleName%"

set "REMOTE_JVMARGS=%SUPPLIED_JVMARGS% %DEFAULT_REMOTE_JVMARGS%"
set "REMOTE_JVMARGS=%REMOTE_JVMARGS:\=/%"
set "REMOTE_JVMARGS=%REMOTE_JVMARGS:"=%"


if "%arg_pos_1%" == "start" (
    call :start_servers
) else if "%arg_pos_1%" == "restart" (
    call :stop_servers
    call :start_servers
) else if "%arg_pos_1%" == "stop" (
    call :stop_servers
) else if "%arg_pos_1%" == "clear" (
    call :clear_all %*
) else if "%arg_pos_1%" == "forceclear" (
    call :clear_all %*
)

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

:get_file_list
set cmdline_args=%*
set "jar_files=%CLASSPATH:;= %"
set "jar_files_cygwin="
for %%A in (%jar_files%) do (
    set "currFile=%%~A"
    set "win_path=!currFile:\=/!"
    if "!win_path:~1,1!"==":" (
        set "drive=!win_path:~0,1!"
        set "path_rest=!win_path:~2!"
        set "cyg_path=/cygdrive/!drive!!path_rest!"
    ) else (
        set "cyg_path=!win_path!"
    )
    set "jar_files_cygwin=!jar_files_cygwin! !cyg_path!"
)
set "jar_files_cygwin=%jar_files_cygwin:~1%"

call :get_value "javax.net.ssl.keyStore" "!cmdline_args:"=""!" "%KEYSTORE%"
set KEYSTORE=%value%
call :get_value "javax.net.ssl.keyStorePassword" "!cmdline_args:"=""!" "%DEFAULT_KEYSTORE_PASSWORD%"
set keyStorePassword=%value%
call :get_value "javax.net.ssl.trustStore" "!cmdline_args:"=""!" "%TRUSTSTORE%"
set TRUSTSTORE=%value%
call :get_value "javax.net.ssl.trustStorePassword" "!cmdline_args:"=""!" "%DEFAULT_TRUSTSTORE_PASSWORD%"
set trustStorePassword=%value%
call :get_value "java.util.logging.config.file" "!cmdline_args:"=""!" "%LOG_PROPERTIES%"
set LOG_PROPERTIES=%value%
call :get_value "log4j.configuration" "!cmdline_args:"=""!" "%LOG4J_PROPERTIES%"
set LOG4J_PROPERTIES=%value%

set "conf_transferrables=%GP_PROPERTIES% %KEYSTORE% %TRUSTSTORE% %LOG_PROPERTIES% %LOG4J_PROPERTIES% %APP_RESOURCES%"
call :print 3 "transferrables=%jar_files% %conf_transferrables%"
goto :EOF

:get_simple_name
set name=%~1
for %%I in ("%name%") do set "name=%%~nxI"
set res=%CONF%\%name%
goto :EOF

:get_value
set "key=%~1"
set "cmdline_args=%~2"
set "default_value_container=%~3"
set "record="
set "value="
set "cmdline_args=!cmdline_args:""="!"
for %%A in (%cmdline_args%) do (
    set a=%%~A
    if "!a!" == "%key%" (
        echo !a! | findstr /C:"%key%" >nul
        if not errorlevel 1 (
            set "record=!a!"
            goto :done
        )
    )
)
:done

if "%record%" == "" (
    for /f "usebackq tokens=*" %%A in ("%GP_PROPERTIES%") do (
        set "line=%%~A"
        if "!line:~0,1!" neq "#" (
            echo !line! | findstr /C:"%key%" >nul
            if not errorlevel 1 (
                set "record=!line!"
                goto :searched
            )
        )
    )
)
:searched
if "%record%"=="" set "record=%default_value_container%"

echo %record% | findstr /C:"=" >nul
if not errorlevel 1 (
    for /f "tokens=2 delims==" %%A in (%record%) do (
        set "temp_value=%%A"
    )
    for /f "token=1 delims= " %%B in ("!temp_value!") do (
        set "value=%%B"
    )
)

if "%value%"=="" (
    set "value=%default_value_container%"
)
goto :EOF

:print
set level=%~1
set "msg=%~2"
if %VERBOSE% geq %level% (
    set i=0

    :loop
    <nul set /p= :
    set /A i+=1
    if %i% lss %level% goto :loop

    echo %msg%

)
if %level% equ 9 (
    exit /b 0
)
goto :EOF


:trim_file_list
set list=%~1
set "existent="
for %%F in (%list%) do (
    if exist %%F (
        set "existent=!existent! %%F"
    )
)
set conf_transferrables=%existent%
goto :EOF

:append_to_ln_cmd
set src_file=%~1
set default=%~2
set unlink_first=%~3
for %%I in ("%src_file%") do set "simple=%%~nxI"
for %%I in ("%default%") do set "simple_default=%%~nxI"

call :get_simple_name %default%
set "link_target=%INSTALL_PATH%\%res%"
if not "%link_target:~0,1%" == "\" (
    set "link_target=~\%link_target%"
)
set link_src=%INSTALL_PATH%\%CONF%\%simple%
if not "%link_src:~0,1%" == "\" (
    set "link_src=~\%link_src%"
)

set "cur_link=ln -fs %link_src% %link_target% "
if not "%unlink_first%"=="" (
    fsutil reparsepoint query "%link_target%" >nul 2>&1
    if not errorlevel 1 (
        del "%link_target%"
    )
    set "cur_link=if exist %link_target% (fsutil reparsepoint query %link_target% >nul 2>&1 & if not errorlevel 1 (del %link_target%)) & %cur_link%"
)

if exist %src_file% if not "%simple%" == "%simple_default%" (
    if "%LINK_CMD%"=="" (
        set "LINK_CMD=;%cur_link%"
    ) else (
        set "LINK_CMD=!LINK_CMD!;%cur_link%"
    )
)
goto :EOF

:clear_all
echo %* | findstr /r /C:"clear[ ]*all" >nul
if not errorlevel 1 (
    echo %* | findstr /r /C:"forceclear[ ]*all" >nul
    if "!errorlevel!" == "1" (
        set /p yn="Are you sure you want to wipe out all paxos state? "
        if "!yn:~0,1!"=="y" set "resp=T"
        if "!yn:~0,1!"=="Y" set "resp=T"
        if "!yn:~0,1!"=="n" set "resp=F"
        if "!yn:~0,1!"=="N" set "resp=F"
        if "!resp!"=="F" (
            exit /b 0
        )
        if "!res!"=="" (
            echo Please answer yes or no.
            exit /b 0
        )
    )
    call :stop_servers
    for %%A in (%servers%) do (
        call :get_address_port %%A
        call !ifconfig_cmd! | find "!address!" >nul 2>&1
        if not errorlevel 1 (
            call :print 3 "%JAVA% %JVMARGS% edu.umass.cs.reconfiguration.ReconfigurableNode clear %%A"
            start /B %JAVA% %JVMARGS% edu.umass.cs.reconfiguration.ReconfigurableNode clear %%A
        ) else (
            echo Clearing state on remote server %%A
            @REM call :print 2 "%SSH% %username%@%address% \""cd %INSTALL_PATH%; nohup %JAVA% %REMOTE_JVMARGS% -cp \`ls jars/*|awk '{printf \$0\"":\""}'\` edu.umass.cs.reconfiguration.ReconfigurableNode clear %server% \"" "
            start /B %SSH% -i %SSH_KEY% %username%@!address! "cd %INSTALL_PATH%; nohup %JAVA% %REMOTE_JVMARGS% -cp `ls jars/*|awk '{printf \$0\"":\""}'` edu.umass.cs.reconfiguration.ReconfigurableNode clear %%A "
        )
    )
) else (
    set matchFound=
    for %%i in (%*) do (
        echo %%i | findstr /i /c:"clear" /c:"forceclear" >nul
        if not errorlevel 1 (
            set matchFound=1
            goto :foundMatch
        )
    )
    :foundMatch
    echo.
    echo The 'clear' and 'forceclear' options can be used only as 'clear all' or 'forceclear all'
)

goto :EOF


:start_servers
if not "%servers%" == "" (
    if not "%APP_ARGS%" == "" (
        echo [%APP% %APP_ARGS%]
    ) else (
        echo [%APP%]
    )
    for %%A in (%servers%) do (
        call :start_server %%A
    )
    if not "%non_local%" == "" (
        if %VERBOSE% gtr 0 (
            echo Ignoring non-local server^(s^) " %non_local%"
        )
    )
)
goto :EOF

:start_server
set server=%~1
call :get_address_port %server%
set "DEBUG_ARGS="
if "%DEBUG_MODE%" == "true" (
    set "DEBUG_ARGS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=%DEBUG_PORT%"
    if %VERBOSE%==2 (
        echo Debug: %server% at %address%:%DEBUG_PORT%
    )
    set DEBUG_PORT+=1
)
%ifconfig_cmd% | findstr /c:"%address%" >nul
if not errorlevel 1 (
    if "%VERBOSE%" == "2" (
        echo %JAVA% %DEBUG_ARGS% %JVMARGS% edu.umass.cs.reconfiguration.ReconfigurableNode %server%
    )
    start /B %JAVA% %DEBUG_ARGS% %JVMARGS% edu.umass.cs.reconfiguration.ReconfigurableNode %server%
) else (
    set "non_local=%server%=%addressport% %non_local%"
    echo Starting remote server %server%
    call :print 1 "Transferring jar files %jar_files% to ""%address%:%INSTALL_PATH%"" "
    @REM call :print 2 "%RSYNC% --rsync-path=""%RSYNC_PATH% and rsync"" --rsh=""%RSYNC_SSH_INFO%"" %jar_files% %username%@%address%:%INSTALL_PATH%/jars/ "
    %RSYNC% --rsync-path="%RSYNC_PATH% && rsync" --rsh="%RSYNC_SSH_INFO%" %jar_files_cygwin% %username%@%address%:%INSTALL_PATH%/jars/ 2>nul
    call :rsync_symlink %address%

    @REM call :print 2 "start /B %SSH% %username%@%address% \""cd %INSTALL_PATH%; nohup %JAVA% %DEBUG_ARGS% %REMOTE_JVMARGS% -cp \`ls jars/*\|awk '{printf \$0\"":\""}'\` edu.umass.cs.reconfiguration.ReconfigurableNode %APP_ARGS% %server% \"" "
    start /B %SSH% %username%@%address% "cd %INSTALL_PATH%; nohup %JAVA% %DEBUG_ARGS% %REMOTE_JVMARGS% -cp `ls jars/*|awk '{printf \$0\"":\""}'` edu.umass.cs.reconfiguration.ReconfigurableNode %APP_ARGS% %server% "

)

goto :EOF

:stop_servers
set "pids="
set "foundservers="
for %%A in (%servers%) do (
    call :get_address_port %%A
    set "server=%%A"
    set "KILL_TARGET=ReconfigurableNode .*%%A"
    call !ifconfig_cmd! | findstr /c:"!address!" >nul
    if not errorlevel 1 (
        set "pid="
        wmic process where "name='java.exe' and CommandLine like '%%!server!%%'" get ProcessId /format:list 2>nul | find "ProcessId=" > temp.txt
        for /f "tokens=2 delims==" %%i in (temp.txt) do (
            set "pid=%%i"
        )
        del temp.txt
        if not "!pid!" == "" (
            set "foundservers=!server!^(!pid!^) !foundservers!"
            set "pids=!pids! !pid!"
        )
    ) else (
        echo Stopping remote server %%A
        @REM echo %SSH% %username%@!address! "kill -9 `ps -ef|grep \""%KILL_TARGET%\""|grep -v grep|awk '{print \$2}'` 2>/dev/null\"""
        start /B %SSH% %username%@!address! "kill -9 `ps -ef|grep \""!KILL_TARGET!\""|grep -v grep|awk '{print \$2}'` 2>/dev/null"
    )
)
if not "%pids%" == "" (
    echo killing "%foundservers%"
    for %%i in (%pids%) do (
        taskkill /PID %%i /F 1>nul 2>&1
    )
)
goto :EOF

:rsync_symlink
set "address=%~1"
call :print 1 "Transferring conf files to %address%:%INSTALL_PATH%"
call :print 2 "%RSYNC% --rsync-path=""%RSYNC_PATH% %LINK_CMD% and rsync"" --rsh=""%RSYNC_SSH_INFO%"" %conf_transferrables% %username%@%address%:%INSTALL_PATH%/%CONF%/"
%RSYNC% --rsync-path="%RSYNC_PATH% %LINK_CMD% && rsync" --rsh="%RSYNC_SSH_INFO%" %conf_transferrables_cygwin% %username%@%address%:%INSTALL_PATH%/%CONF%/ 2>nul
goto :EOF

:get_address_port
set "server=%~1"
set "addressport="
for /f "usebackq tokens=*" %%A in ("%GP_PROPERTIES%") do (
    set "line=%%A"
    if "!line:~0,1!" neq "#" (
        echo !line! | findstr /r /c:"^[ \t]*%ACTIVE_KEYWORD%.%server%=" >nul
        if not errorlevel 1 (
            for /f "tokens=2 delims==" %%B in ("!line!") do (
                set "addressport=%%~B"
            )
        )
        if "!addressport!" == "" (
            echo !line! | findstr /r /c:"^[ \t]*%RECONFIGURATOR_KEYWORD%.%server%=" >nul
            if not errorlevel 1 (
                for /f "tokens=2 delims==" %%B in ("!line!") do (
                    set "addressport=%%~B"
                )
            )
        )
    )
)
if "!addressport!" == "" (
    set "non_existent=%server% %non_existent%"
    goto :EOF
)
set "address="
for /f "delims=:" %%B in ("!addressport!") do (
    set "address=%%B"
)
set "ifconfig_cmd=netsh interface ip show config"
goto :EOF



endlocal