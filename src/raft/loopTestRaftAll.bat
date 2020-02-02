@echo off
for /l %%i in (1,1,10) do (
    echo %%i
    go test -run 2
)