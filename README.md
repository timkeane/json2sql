# json2sql

Creates SQL scripts from a GeoJSON `FeatureCollection`.

To configure the script you must include a `.env` file in the root of this project. There are 2 options for configuring the script environment:

## Option 1 (from a geoserver WFS):
```sh
PROTOCOL=http
HOST=hostname
PORT=8080
# The geoserver namespace in which configured layers are found
NAMESPACE=carto
# A list of layer names served by the WFS
LAYERS=["BACKGROUND_SDO","BOROUGH_BORDER_SDO","BOROUGH_POINT_SDO","HYDRO_LABELLINE_SDO","HYDRO_LABELPOINT_SDO","HYDRO_LABELPOLY_SDO","LANDMASSFRINGE_SDO","LANDMASSPANGAEAWET_SDO","LOW_RES_ROAD_SDO","NEIGHBORHOOD_POINT_SDO","ROAD_NATEARTH_SDO","ROAD_TIGER_SDO"]
# The output folder for generated SQL scripts
OUT=./sql/basemap/
```

## Option 2 (from a GeoJSON source other than geoserver - i.e. NYC Open Data):
```sh
# A list of objects containing a `name` property for the layer name and a `url` property for the GeoJSON FeatureCollection
LAYERS=[{"name":"PARK","url":"https://data.cityofnewyork.us/api/views/enfh-gkve/rows.geojson?date=20240709&accessType=DOWNLOAD"}]
# The output folder for generated SQL scripts
OUT=./sql/planimetric/
```


`npm install`

`node json2sql.js`
