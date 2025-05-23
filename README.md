# PROJECT 4 - Analysis of AISdata with Pyspark
Saule Trepekunaite

## 1.1 Objective
Leverage the capabilities of PySpark to analyze maritime data and identify the vessel that has traveled the longest route on a specific day. This assignment will help gain practical experience with big data technologies, particularly Apache Spark, and its Python API, PySpark.

## 1.2 Requirements
Started by installing Apache Spark and Hadoop. For Apache Spark I followed the following tutorial: https://www.youtube.com/watch?v=_jFj30A3L3k

## 1.3 Data
The provided dataset aisdk-2024-05-04.csv contains Automatic Identification System (AIS) data for vessels, including details such as MMSI (Maritime Mobile Service Identity), timestamp, latitude, and longitude.


# 2.0 Data Preparation
To load the data into a PySpark DataFrame, I first created a SparkConf object to configure the Spark application. I set the Master to local becaue I want to run this application locally on one machine. I then create a SparkContext object to connect to the Spark cluster. SparkContext is the entry point to any Spark functionality. It allows me to create RDDs, accumulators, and broadcast variables. This is the setup:

```
# Set up the Spark configuration and context
conf = SparkConf().setMaster("local").setAppName("VesselDistTravel")
sc = SparkContext(conf=conf) 

input = sc.textFile("aisdk-2024-05-04.csv") 
```

With input I read the AIS data. I choose to read it as text file since the data is in a csv file and I will parse it late. input is a Resilient Distributed Dataset (RDD) which is a distributed collection of data that can be processed in parallel.

The first line of the AIS data is the header. This text line is not a record and will therefore cause errors if not removed.

```
# Skip first line (header)
data_no_header = input.zipWithIndex().filter(lambda x: x[1] > 0).map(lambda x: x[0])
```
zipWithIndex() adds an index to each line, this makes the first line easy to call. filter(lambda x: x[1] > 0) filters out the first line by returning only lines with index > 0. map(lambda x: x[0]) returns the headerless data without the indexing.

Next I extracted only the collums of data I need:
```
def extractVesselDistance(line):
    ''' Extract the MMSI, timestamp, latitude, and longitude from each line of data.'''
    fields = line.split(",")
    mmsi = int(fields[2])
    timestamp = fields[0].split(" ")[1] # Split the timestamp to get the time
    latitude = float(fields[3])
    longitude = float(fields[4])
    return (mmsi, (timestamp, latitude, longitude)) 

mappedInput = data_no_header.map(extractVesselDistance) 
```
Here I ensure that the data types for latitude, longitude, and timestamp are appropriate for calculations and sorting by setting MMSI to int, timestamp to string, and latitude and longitude to floats. I also remove the date from the timestamp as the data is all from the same day anyway. The function returns a tuple of (MMSI, (timestamp, latitude, longitude)). So MMSI is key and (timestamp, latitude, longitude) is value.

Then I grouped the data by key (MMSI) to get all the positions of each vessel in a iterable. I sort the positions of each vessel by timestamp so that why I calculate the distances between records I will be calculating them correctly.

```
grouped = mappedInput.groupByKey()
grouped = grouped.mapValues(lambda positions: sorted(positions, key=lambda x: x[0]))
```
mapValues takes the value of the tuple and sorts it by timestamp. lambda x: x[0] indicates that I am sorting by the first element of the tuple (timestamp). We don't have to define positions because it is gets defined in the mapValues function as a list of tuples (timestamp, latitude, longitude) for each MMSI.

# 3.0 Data Processing with PySpark

## 3.1 Shifting the Coordinates
To calculate the distance between consecutive positions for each vessel I need to paste the coordinates in a new collum and shift these values down. I wanted to have the "old" and "new" coordinates in the same line of data so that the individual distances could be calculated within each line, meaning that the calculations could be done in parallel. 

To paste and shift the coordinates I define the following function:
```
def shift_coordinates(positions):
    ''' Shift the coordinates down one row '''
    positions = list(positions)
    shifted_positions = []
    for i in range(len(positions) - 1):
        if i == 0:
            timestamp1, lat1, lon1 = positions[0]
            timestamp2, lat2, lon2 = positions[0]
        else:
            timestamp1, lat1, lon1 = positions[i]
            timestamp2, lat2, lon2 = positions[i - 1]
        shifted_positions.append((timestamp1, lat1, lon1, timestamp2, lat2, lon2))
    return shifted_positions
```
## 3.2 Calculate Distances with Haversine
Now I need to define a function that will calculate the distance between two points on the earth given their latitude and longitude. For this I will use the Haversine formula.
```
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
```
With this I am ready to calculate the distances between each record for every MMSI. 
```
def calculate_distance(positions):
    # Calculate the distance between the two points
    distances = []
    for timestamp1, lat1, lon1, timestamp2, lat2, lon2 in positions:
        distance = haversine(lat1, lon1, lat2, lon2)
        distances.append((timestamp1, lat1, lon1, timestamp2, lat2, lon2, distance))
    return distances
```
The function returns a list of tuples with the coordinates of the two points and the distance between them. If I wanted to check that this is working as expected I could take a look at the data of three vessels with the following: 
```
grouped = grouped.mapValues(shift_coordinates) # Shift the coordinates down one row
grouped = grouped.mapValues(calculate_distance) # Calculate the distance between the two points

results = grouped.take(3) # Show the records of the first 3 vessels

for mmsi, records in results:
    print(f"MMSI: {mmsi}")
    for record in records:
        timestamp1, lat1, lon1, timestamp2, lat2, lon2, distance = record
        print(f"Timestamp1: {timestamp1}, Latitude1: {lat1}, Longitude1: {lon1}, Timestamp2: {timestamp2}, Latitude2: {lat2}, Longitude2: {lon2}, Distance: {distance} km")
```
This will print the three vessles MMSI and all their records of timestamp, latitude, longitude, previous timestamp, latitude, longitude, and distance between the two points.

## 3.3 Total Distance
By summing all the distances between each record for every MMSI, we get the distance travelled for each vessle.
```
def sum_distances(positions):
    ''' Sum the distances for each vessel. '''
    positions = list(positions)
    total_distance = 0
    for item in positions:
        total_distance += item[-1]  # item[-1] is the distance value
    return [total_distance]
```
This function simply returns the total distance traveled, since we no longer need the timestamps and coordinates. We therefore add one more line before defining our results:
```
grouped = grouped.mapValues(sum_distances) # Sum the distances
```
Then we can extract all vessels and print only their MMSI and total distance traveled:
```
results = grouped.collect() # Collect the results

for mmsi, records in results:
    print(f"MMSI: {mmsi}")
    for record in records:
        vessle_dist = record
        print(f"vessle_dist: {vessle_dist} km")
```

# 4.0 Identifying the Longest Route
To find whitch vessel traveled the longest we sort our results by distance in descending order.
```
#sorted_results = sorted(results, key=lambda x: x[1], reverse=True)
for mmsi, distance in sorted_results:
     print(f"MMSI: {mmsi}, Distance: {distance} km")
```
But from this we see that some vesseles travelles huge and unrealistic distances. It is not realistic for a vessle to travel more than 800 km (and still this is high). I therefore filter the results to only show vessels with a distance less than 800 km.
```
sorted_results = sorted(results, key=lambda x: x[1][0], reverse=True)
for mmsi, distance_list in sorted_results:
    distance = distance_list[0]
    if distance < 800:
        print(f"MMSI: {mmsi}, Distance: {distance} km")
    else:
        pass
        #print(f"MMSI: {mmsi}, Distance: {distance} km (too high)")
```
We need to define a new sorted_results variable because we need the index of the distance for the filter. 

At the end of the code I stop the Spark context with
```
sc.stop()
```
This will stop the Spark context and free up resources.

# 5.0 Output
The final output from the code is now the MMSI of each vessel and the distance it has traveled. The first record will be the vessle that has the highest (realistic) traveldistance, and the rest of the vessels will come under with their MMSI and distance traveled in decending order. But this is an extreemly long list, so to avoid having to scroll past all the vessels we can add a count:
```
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
```
This will give us only the 10 vessles that have the highest realistic traveldistance. If we want to see only the top one, edit 
```
if count == 1:
```
From this we get

MMSI: 219133000, Distance: 792.82190199522 km  

So the vessle with Maritime Mobile Service Identity 219133000 has traveled the longest realistic distance on 04. April 2024. 

If you would like to see all the recods, regardless if the result is realistic or not, just remove all the # after else and comment out pass. The longest distance traveled was:

MMSI: 219000962, Distance: 91467.68949836631 km (too high)

