@echo off
for /l %%i in (1,1,10) do (
    echo %%i
    rem go test -run TestPersistPartitionUnreliableLinearizable3A
    rem if %errorlevel% == 0 (
    rem     echo successfully
    rem ) else (
    rem     echo failed
    rem )

    rem go test -run TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B
    rem if %errorlevel% == 0 (
    rem     echo successfully
    rem ) else (
    rem     echo failed
    rem )
    go test -run 3
)