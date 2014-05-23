#!/usr/bin/python
import os
import sys
args = sys.argv
f1=open(args[1], "r")
f2=open("result_tmp","w")
for i in f1:
	if len(i)>1:
		i=i.replace("\n",",")
		f2.write(i);
	else:
		f2.write(i)
f1.close()
f2.close()
f2=open("result_tmp","r")
f3=open(args[1],"w")
for j in f2:
	if "TYPE: BGP4MP/MESSAGE/Update,FROM: 213.144.128.203 AS13030,TO: 128.223.51.102 AS6447," in j:
		#PRint j
		continue;
	else:
		#j=j+"\n"
		j=j.replace(",","\n");
		f3.write(j)
f2.close()
f3.close()

os.system('rm result_tmp')
