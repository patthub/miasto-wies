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
        return "Miejsce poza zaborami Polski"





#print(check_place_status(20.233, 53.844))

