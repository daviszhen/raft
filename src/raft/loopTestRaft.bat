@echo off
for /l %%i in (1,1,10) do (
    echo %%i
    rem go test -run 2
    go test -run Unreliable
    go test -run Reliable
)