import sqlite3
import pandas as pd

conn = sqlite3.connect('nuclear_outages.db')

query = '''
SELECT 
    f.facility_name, 
    r.generator_num, 
    r.date, 
    r.capacity_mw, 
    r.outage_mw, 
    r.percent
FROM Outage_report r
JOIN Facility f ON r.facility_id = f.facility_id
JOIN Generator g ON r.facility_id = g.facility_id AND r.generator_num = g.generator_num
WHERE f.facility_name LIKE '%Palo Verde%' 
ORDER BY r.date DESC
LIMIT 10;
'''

df_resultado = pd.read_sql_query(query, conn)
print(df_resultado)

conn.close()