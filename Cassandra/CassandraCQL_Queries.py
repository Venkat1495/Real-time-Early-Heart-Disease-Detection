from cassandra.cluster import Cluster
cluster = Cluster(['0.0.0.0'], port=9042)
section = cluster.connect("cas")

print("Reading Data from Cassandra : ************")

rows = section.execute('select * from Heart_Prediction_data;')
for heat_data_row in rows:
    print(heat_data_row)