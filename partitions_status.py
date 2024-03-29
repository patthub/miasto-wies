import geopandas as gpd
# import gdal
from shapely.geometry import Point, Polygon
from pyproj import Proj, transform
import geopy.distance
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=DeprecationWarning)



gdf_galicja = gpd.read_file("galicja_mercator.shp")
gdf_prusy = gpd.read_file("prusy_mercator.shp")
gdf_all = gpd.read_file("zabory_all_mercator.shp")


polygon_prusy = gdf_prusy['geometry'][0]
polygon_galicja = gdf_galicja['geometry'][0]
polygon_all = gdf_all['geometry'][0]




def transform_pnt(longitude: float, latitude: float) -> Point:
    raw_pnt = Point(longitude, latitude)
    transformed_pnt = (transform(Proj(init='epsg:4326'), Proj(init='epsg:3857'), longitude, latitude))
    return Point(transformed_pnt)

def check_place_status(longitude: float, latitude: float) -> str:
    pnt = transform_pnt(longitude, latitude)
    if polygon_prusy.contains(pnt):
        return "Zabór: Prusy"
    elif polygon_galicja.contains(pnt):
        return "Zabór: Galicja"
    elif polygon_all.contains(pnt):
        return "Zabór: Rosja"
    else:
        return "Zagranica"

def check_if_poland(longitude: float, latitude: float) -> bool:
    pnt = transform_pnt(longitude, latitude)
    if any([polygon_prusy.contains(pnt), polygon_galicja.contains(pnt), polygon_all.contains(pnt)]):
        return True
    else: return False    
    
    
def get_mean_of_points(points: list) -> float:

    sum_long = 0
    sum_lat = 0
    mean_long = 0
    mean_lat = 0
    length = len(points)
    
    for elem in points:
        sum_long += elem[0]
        sum_lat += elem[1]
      
    mean_long = sum_long / length
    mean_lat = sum_lat / length
    
    return (mean_long, mean_lat)


from math import cos, asin, sqrt

def distance(lat1, lon1, lat2, lon2):
    p = 0.017453292519943295
    hav = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p)*cos(lat2*p) * (1-cos((lon2-lon1)*p)) / 2
    return 12742 * asin(sqrt(hav))

def closest(data, v):
    return min(data, key=lambda p: distance(v['lat'],v['lon'],p['lat'],p['lon']))

tempDataList = [{'lat': 39.7612992, 'lon': -86.1519681}, 
                {'lat': 39.762241,  'lon': -86.158436 }, 
                {'lat': 39.7622292, 'lon': -86.1578917}]

v = {'name': 'agnieszka', 'lat': 39.7622290, 'lon': -86.1519750}
print(closest(tempDataList, v))


#print(check_place_status(20.233, 53.844))


# test = {k:v for k,v in places_json.items() if not v.get('partition')}

# result = {}
# for k,v in test.items():
#     result.update({k: check_place_status(float(v.get('lng')), float(v.get('lat')))})
    
# with open('miejsca_urodzenia_zabory.json', 'w') as f:
#     json.dump(result, f, ensure_ascii=False)

# check_place_status(float('30.31413'), float('59.93863'))
















