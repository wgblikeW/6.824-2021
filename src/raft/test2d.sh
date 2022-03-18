#!/bin/bash

echo "TestSnapshotBasic2D"
go test -run ^TestSnapshotBasic2D$ | grep ok
echo "TestSnapshotInstall2D"
go test -run ^TestSnapshotInstall2D$ | grep ok
echo "TestSnapshotInstallUnreliable2D"
go test -run ^TestSnapshotInstallUnreliable2D$ | grep ok
echo "TestSnapshotInstallCrash2D"
go test -run ^TestSnapshotInstallCrash2D$ | grep ok
echo "TestSnapshotInstallUnCrash2D"
go test -run ^TestSnapshotInstallUnCrash2D$ | grep ok