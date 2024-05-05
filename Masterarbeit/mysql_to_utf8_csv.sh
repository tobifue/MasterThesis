#!/bin/bash

mysql -h mafuehles.mysql.database.azure.com -u t_fuehles -p12Compexpert1994- -D ma -e "SELECT * FROM household_demographics" --batch --raw --skip-column-names | sed 's/\t/,/g' | iconv -f ISO-8859-1 -t UTF-8 > household_demographics.csv