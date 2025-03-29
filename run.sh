#/bin/bash

make set k=foo p=6000
make set k=fo1 p=6000

make set k=fo2 p=6001
make set k=fo3 p=6001

make replica-of lp=6001 p=6000

make keys p=6000
