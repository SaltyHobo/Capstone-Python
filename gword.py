import sqlite3
import time
import zlib
import string

conn = sqlite3.connect('airq3.sqlite')
cur = conn.cursor()

#future update, values=list() from the start
cur.execute('SELECT County.name, Yr03.value FROM County JOIN Yr03 ON County.id = Yr03.county_id ORDER BY value DESC')
values = dict()
for val_row in cur :
    values[val_row[0]] = val_row[1]
    #print(values)
    #print(values[val_row[0]])
    #values = val_row

lvalues = list(values.items())

ordered = sorted(lvalues, key=lambda x: x[1], reverse=True)

top100 = ordered[0:100]

highest = top100[0][1]
print(highest)
lowest = top100[99][1]
print(lowest)

# Spread the font sizes across 20-100 based on the count
bigsize = 50
smallsize = 15

fhand = open('gword.js','w')
fhand.write("gword = [")
first = True
for k in top100:
    if not first : fhand.write( ",\n")
    first = False
    print(k)
    size = k[1]
    size = (size - lowest) / float(highest - lowest)
    size = int((size * bigsize) + smallsize)
    fhand.write("{text: '"+k[0]+"', size: "+str(size)+"}")
fhand.write( "\n];\n")
fhand.close()

print("Output written to gword.js")
print("Open gword.htm in a browser to see the vizualization")
