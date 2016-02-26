#!/bin/bash
echo 'Shutting down hungryhippos java processes on this node'
for processid in `cat hungryhippos_java_processes_to_kill.txt`
 do
    kill pid $processid > ./system.out 2>./system.err &
 done
