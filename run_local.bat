@echo off
SETLOCAL EnableDelayedExpansion

echo ==============================================================
echo  Weather Anomaly Detection - Local Runner (Windows)
echo ==============================================================

:: Check Java
java -version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Java is not installed or not in PATH. Please install JDK 8 or 11.
    exit /b 1
)

:: Check Maven
call mvn -version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Maven is not installed or not in PATH. Please install Apache Maven.
    exit /b 1
)

:: Generate sample data if not present
if not exist sample_data.csv (
    echo [INFO] Generating sample_data.csv using Python...
    python generate_sample_data.py
    if !errorlevel! neq 0 (
        echo [WARNING] Failed to run python script. Make sure Python is installed.
    )
)

:: Clean up previous outputs
if exist stage1_output rmdir /s /q stage1_output
if exist stage2_output rmdir /s /q stage2_output

echo.
echo [INFO] Compiling and running MapReduce pipeline locally using Maven...
echo.

:: Run using Maven with classpathScope=test to include provided dependencies
call mvn compile exec:java -Dexec.classpathScope="test" -Dexec.mainClass="com.pdc.weather.WeatherDriver" -Dexec.args="sample_data.csv stage1_output stage2_output"

if %errorlevel% neq 0 (
    echo.
    echo [ERROR] MapReduce execution failed!
    exit /b %errorlevel%
)

echo.
echo ==============================================================
echo  Pipeline Completed Successfully!
echo ==============================================================
echo.
echo Stage 1 Output (Baseline Stats - Mean, Sigma):
echo -------------------------------------------------------------
if exist stage1_output\part-r-00000 (
    type stage1_output\part-r-00000
) else (
    echo [ERROR] stage1_output\part-r-00000 not found!
)

echo.
echo Stage 2 Output (Detected Anomalies - Temp > 3 Sigma):
echo -------------------------------------------------------------
if exist stage2_output\part-m-00000 (
    type stage2_output\part-m-00000
) else (
    echo [ERROR] stage2_output\part-m-00000 not found!
)
echo.
echo Note: If there are warning messages about winutils.exe, they are normal for Hadoop on Windows and can be ignored.
pause
