#/bin/bash

make set k=foo p=6379
make set k=fo1 p=6379

make set k=fo2 p=6478
make set k=fo3 p=6478

make replica-of lp=6478 p=6379

make keys p=6379
