#!/bin/bash


GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

bwa index ecoli_ref.fna

bwa mem ecoli_ref.fna SRR33602302.fastq > aligned_reads.sam

FLAGSTAT=$(samtools flagstat aligned_reads.sam)
echo "${FLAGSTAT}"

PRCNTG=$(echo "$FLAGSTAT" | grep -oP 'mapped \(\K[\d.]+(?=%)' | head -1)

echo -e "Mapped ${PRCNTG}%"
if (( $(echo "$PRCNTG >= 90.0" | bc -l) )); then
	echo -e "${GREEN}OK${NC}"
else
	echo -e "${RED}Not OK${NC}"
fi
