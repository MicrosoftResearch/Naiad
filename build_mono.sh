#!/bin/sh

# To build in Debug mode. Binaries will be placed in $PROJ/bin/Debug.
xbuild /p:Configuration="MonoDebug"

# To build in Release mode. Binaries will be placed in $PROJ/bin/Release.
#xbuild /p:Configuration="MonoRelease"
