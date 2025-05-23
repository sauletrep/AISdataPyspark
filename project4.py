from pyspark import SparkConf, SparkContext
from math import radians, sin, cos, sqrt, atan2

conf = SparkConf().setMaster("local").setAppName("VesselDistTravel")
sc = SparkContext(conf=conf) 
input = sc.textFile("aisdk-2024-05-04.csv") 

# Skip first line (header)
data_no_header = input.zipWithIndex().filter(lambda x: x[1] > 0).map(lambda x: x[0])

def extractVesselDistance(line):
    ''' Extract the MMSI, timestamp, latitude, and longitude from each line of data.'''
    fields = line.split(",")
    mmsi = int(fields[2])
    timestamp = fields[0].split(" ")[1] # Split the timestamp to get the time
    latitude = float(fields[3])
    longitude = float(fields[4])
    return (mmsi, (timestamp, latitude, longitude)) # return a tuple of (MMSI, (timestamp, latitude, longitude))
    # MMSI is key and (timestamp, latitude, longitude) is value

mappedInput = data_no_header.map(extractVesselDistance) # Map the input data to (MMSI, (timestamp, latitude, longitude))
grouped = mappedInput.groupByKey() # Group by MMSI
grouped = grouped.mapValues(lambda positions: sorted(positions, key=lambda x: x[0])) # Sort by timestamp

def shift_coordinates(positions):
    ''' Paste and shift the coordinates down one row '''
    positions = list(positions)  # Convert ResultIterable to list
    shifted_positions = []
    for i in range(len(positions) - 1):
        if i == 0:
            # If it's the first element, we just paste the first element so the distance is 0
            timestamp1, lat1, lon1 = positions[0]
            timestamp2, lat2, lon2 = positions[0]
        else:
            # For all other elements, we paste the current element with the previous one
            timestamp1, lat1, lon1 = positions[i]
            timestamp2, lat2, lon2 = positions[i - 1]
        shifted_positions.append((timestamp1, lat1, lon1, timestamp2, lat2, lon2))
    return shifted_positions

def haversine(lat1, lon1, lat2, lon2):
    ''' Haversine formula to calculate the distance between two points on the earth given their latitude and longitude. '''
    R = 6371.0 # Radius of the earth in km
    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c # Distance in km
    return distance

def calculate_distance(positions):
    ''' Calculate the distance between the two points. '''
    distances = []
    for timestamp1, lat1, lon1, timestamp2, lat2, lon2 in positions:
        distance = haversine(lat1, lon1, lat2, lon2)
        distances.append((timestamp1, lat1, lon1, timestamp2, lat2, lon2, distance))
    return distances

def sum_distances(positions):
    ''' Sum the distances for each vessel. '''
    positions = list(positions)
    total_distance = 0
    for item in positions:
        total_distance += item[-1]  # item[-1] is the distance value
    return [total_distance]

grouped = grouped.mapValues(shift_coordinates) # Shift the coordinates down one row
grouped = grouped.mapValues(calculate_distance) # Calculate the distance between the two points
grouped = grouped.mapValues(sum_distances) # Sum the distances
 
#results = grouped.take(3) # Show the first 3 MMSI records
results = grouped.collect() # Collect the results for all vessels

# Sort the results by distance in descending order
sorted_results = sorted(results, key=lambda x: x[1][0], reverse=True)

count = 0
for mmsi, distance_list in sorted_results:
    distance = distance_list[0]
    if distance < 800:
        print(f"MMSI: {mmsi}, Distance: {distance} km")
        count += 1
        if count == 10:
            break
    else:
        pass
        #print(f"MMSI: {mmsi}, Distance: {distance} km (too high)")
        #count += 1
        #if count == 10:
        #    break

sc.stop()
# --- This is the end of the code ---