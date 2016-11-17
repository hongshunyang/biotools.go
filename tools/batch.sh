#!/bin/bash
./app.py -i ./../data/20150503/BP/FLM-H2-BP.csv -g ./../result/BP_descendant_ancestor.csv -c BP-GOTERM.txt -t goterm
./app.py -i ./../data/20150503/CC/CDS-7-CC.csv -g ./../result/CC_descendant_ancestor.csv -c CC-GOTERM.txt -t goterm
./app.py -i ./../data/20150503/MF/CDS-7-MF.csv -g ./../result/MF_descendant_ancestor.csv -c MF-GOTERM.txt -t goterm
