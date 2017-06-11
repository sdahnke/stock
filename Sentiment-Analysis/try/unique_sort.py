import csv

# Alle Ticker sollen unique sortiert in einer neuen csv-Datei ausgegeben werden

Ticker = []
reader = csv.reader('./input/news_reuters.csv')
for row in reader:
    if row[0] not in Ticker:
        Ticker.append(row[0])


#for row in reutersfile:
#    if row[0] not in Ticker:
#        Ticker.append(row[0])

#    DataCaptured = csv.reader('./input/news_reuters.csv', delimiter=',', skipinitialspace=True)

#    Ticker = []
#    for row in DataCaptured:
#        if row[0] not in Ticker:
#            Ticker.append(row[0])

#    print Ticker

print Ticker