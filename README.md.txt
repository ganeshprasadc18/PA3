*submitted by*
----------------
Ganesh Prasad Chandra Shekar - A20557831		
Ramya Venkatesh              - A20546964

1. Generate datasets using Gensort example
------------------------------------------
./gensort -a 10000000 inputfile_1GB

   ELSE

generatefiles.sh will generate all the required datasets


This command generates ascii records of size 1GB and generates a output file with the name "inputfile_1GB"

2. To perform Linux sort
-----------------------------------------
time sort -k 1 inputfile_1GB -o sorted1GBlinux.txt --parallel=8 > linsort1GB.log 2>&1

This command runs sorting on 1GB file named 'inputfile_1GB' and outputs sorted1GBlinux.txt and a log file with name 'linsort1GB.log' 

3. To perform C program Shared-Memory Terasort - mysort.c
---------------------------------------------------------
*first compile the code using the command below-> 

	gcc -mysort.c -o mysort


ELSE

Run makefile to compile C program - mysort.c

*Post compiling, run the executable in the below format ->

        ./mysort <input file> <output file> <number of threads>

Example:./mysort inputfile_1GB sorted1GBmysort.txt 8 >> mysort1GB.log 2>&1


