import geopandas as gpd
import gdal
from shapely.geometry import Point, Polygon
from pyproj import Proj, transform
from geopy.distance import geodesic as GD

from partitions_status import transform_pnt



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




        