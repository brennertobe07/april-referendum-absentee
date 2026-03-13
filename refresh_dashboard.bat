@echo off
title April Referendum Absentee Dashboard — Data Refresh

echo.
echo ============================================================
echo   APRIL REFERENDUM ABSENTEE DASHBOARD — DATA REFRESH
echo ============================================================
echo.
echo Running Python script to pull from INSTANCE-1...
echo.

py -3.12 "C:\Scripts\Python\Python_Absentee\April\april-referendum-absentee\build_april_absentee_json.py"

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo *** ERROR: Script failed. See message above. ***
    echo.
    pause
    exit /b 1
)

echo.
echo ============================================================
echo   Data files updated successfully!
echo.
echo   NEXT STEP: Open GitHub Desktop and:
echo     1. Review the changed data\ files
echo     2. Add commit summary (e.g. "refresh 3/13")
echo     3. Click Commit to main
echo     4. Click Push origin
echo.
echo   The live dashboard will update within ~1 minute.
echo ============================================================
echo.

pause
