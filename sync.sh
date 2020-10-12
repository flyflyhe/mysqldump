#!/bin/bash

originList=("203")

# shellcheck disable=SC2068
for var in ${originList[@]};
do
     echo $var
     rsync -avzP $var:/backup ./
done