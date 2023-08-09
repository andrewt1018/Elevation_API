# Elevation_API
An API that takes in a pair of coordinates in the United States (lat, long) and returns the elevation from sea level of that coordinate. Uses the USGS public s3 buckets <a href="https://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Elevation/1m/Projects/">here</a>.

Runs on the localhost at: http://127.0.0.1:8000

Sample query: http://localhost:8000/elevation_api?lat=-83.938675&long=30.11062777777778
