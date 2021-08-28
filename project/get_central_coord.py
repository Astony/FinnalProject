import math


def get_coordinates_of_central(dataframe):
    """Function that calculates teh center area with hotels"""
    x = 0.0
    y = 0.0
    z = 0.0

    for i, coord in dataframe.iterrows():
        latitude = math.radians(float(coord.Latitude))
        longitude = math.radians(float(coord.Longitude))

        x += math.cos(latitude) * math.cos(longitude)
        y += math.cos(latitude) * math.sin(longitude)
        z += math.sin(latitude)

    total = len(dataframe)

    x = x / total
    y = y / total
    z = z / total

    central_longitude = math.atan2(y, x)
    central_square_root = math.sqrt(x * x + y * y)
    central_latitude = math.atan2(z, central_square_root)
    return math.degrees(central_latitude), math.degrees(central_longitude)
