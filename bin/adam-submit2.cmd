@echo off
setlocal enabledelayedexpansion

REM Licensed to Big Data Genomics (BDG) under one
REM or more contributor license agreements.  See the NOTICE file
REM distributed with this work for additional information
REM regarding copyright ownership.  The BDG licenses this file
REM to you under the Apache License, Version 2.0 (the
REM "License"); you may not use this file except in compliance
REM with the License.  You may obtain a copy of the License at
REM
REM     http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing, software
REM distributed under the License is distributed on an "AS IS" BASIS,
REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM See the License for the specific language governing permissions and
REM limitations under the License.
REM

REM usage: adam-submit [<spark-args> --] <adam-args>


REM Split args into Spark and ADAM args
REM DD is "double dash"
set DD=False
set PRE_DD=
set POST_DD=
for %%i in (%*) do (
  if %%i == -- (
    set DD=True
  ) else (
    if !DD!==False (
      set PRE_DD=!PRE_DD! %%i
    ) else (
      set POST_DD=!POST_DD! %%i
    )
  )
)

if !DD!==True (
  set SPARK_ARGS=!PRE_DD!
  set ADAM_ARGS=!POST_DD!
) else (
  set SPARK_ARGS=
  set ADAM_ARGS=!PRE_DD!
) 

REM Figure out where ADAM is installed
set SCRIPT_DIR=%~dp0..

REM does the user have ADAM_OPTS set? if yes, then warn
if "x%ADAM_OPTS"=="x" (
    echo "WARNING: Passing Spark arguments via ADAM_OPTS was recently removed."
    echo "Run adam-shell instead as adam-shell <spark-args>"
)

REM Find ADAM cli assembly jar
set ADAM_CLI_JAR=
if EXIST "%SCRIPT_DIR%\repo" (
  set ASSEMBLY_DIR=%SCRIPT_DIR%\repo
) else (
  set ASSEMBLY_DIR=%SCRIPT_DIR%\adam-assembly\target
)

REM count the number of adam jars found
set NUM_JARS=
for /f "delims=" %%i in ('dir /a-d /b "%ASSEMBLY_DIR%\adam*.jar" ^
			  ^| find /v "javadoc" ^
			  ^| find /c /v "source"') do set NUM_JARS=%%i

if x%NUM_JARS% == x0 (
  echo "Failed to find ADAM assembly in %ASSEMBLY_DIR%."
  echo "You need to build ADAM before running this program."
  exit 1
)

set ASSEMBLY_JARS=
for /f "delims=" %%i in ('dir /a-d /b "%ASSEMBLY_DIR%\adam*.jar" ^
			  ^| find /v "javadoc" ^
			  ^| find /v "source"') do set ASSEMBLY_JARS=%%i

if not x%NUM_JARS% == x1 (
  echo "Found multiple ADAM cli assembly jars in %ASSEMBLY_DIR%:"
  echo "%ASSEMBLY_JARS%"
  echo "Please remove all but one jar."
  exit 1
)

set ADAM_CLI_JAR=%ASSEMBLY_DIR%\%ASSEMBLY_JARS%

REM Allow main class to be overridden
if not defined ADAM_MAIN (
  set ADAM_MAIN=org.bdgenomics.adam.cli.ADAMMain
)
echo Using ADAM_MAIN=%ADAM_MAIN%

set SPARK_SUBMIT=
if not defined SPARK_HOME (
  for /f "delims=" %%i in ('where spark-submit.cmd') do set SPARK_SUBMIT=%%i
) else (
  set SPARK_SUBMIT=%SPARK_HOME%\bin\spark-submit.cmd
)

if not defined SPARK_SUBMIT (
  echo "SPARK_HOME not set and spark-submit not on PATH; Aborting."
  exit 1
)

echo Using SPARK_SUBMIT=%SPARK_SUBMIT%

REM submit the job to Spark
%SPARK_SUBMIT% ^
    --class %ADAM_MAIN% ^
    --conf spark.driver.extraClassPath=%ADAM_CLI_JAR% ^
    --conf spark.executor.extraClassPath=%ADAM_CLI_JAR% ^
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer ^
    --conf spark.kryo.registrator=org.bdgenomics.adam.serialization.ADAMKryoRegistrator %SPARK_ARGS% %ADAM_CLI_JAR% %ADAM_ARGS%
