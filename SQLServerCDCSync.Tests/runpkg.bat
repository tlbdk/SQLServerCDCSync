@echo off
setlocal

dtexec.exe /SQL "CDC Initial Load Package for SQLServerCDCSyncDestination" /SERVER localhost /USER sa /PASSWORD Qwerty1234 /REPORTING I
dtexec.exe /SQL "CDC Merge Load Package for SQLServerCDCSyncDestination" /SERVER localhost /USER sa /PASSWORD Qwerty1234 /REPORTING I