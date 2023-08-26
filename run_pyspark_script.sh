#!/bin/bash

source .env

echo "Corriendo el programa: " $1
echo -e "\n*********\n*********\n*********\n"

$SPARK_SUBMIT_PATH $1

echo -e "\n**********\nTERMINADO\n"