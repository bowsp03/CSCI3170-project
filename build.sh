#!/bin/bash
FILE="DatabaseConnection.java"
CLASS="DatabaseConnection.class"
read -p "Delete $CLASS? " yn
while true; do
    case $yn in
        [Yy]* ) rm -f $CLASS; break;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no."; read -p "Delete $CLASS? " yn;;
    esac
done
javac $FILE
if [ $? -eq 0 ]; then
    echo "Build successful"
fi

