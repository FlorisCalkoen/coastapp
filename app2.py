
import dotenv
import geopandas as gpd
import geoviews as gv
import holoviews as hv
import hvplot.pandas  # noqa
import panel as pn
import pyproj
from holoviews import streams
from shapely import wkt

dotenv.load_dotenv(override=True)



if __name__ == "__main__":

    def prepare_default_geometry(data, crs):
        """
        Prepares a default geometry from a data dictionary and sets its CRS This should
        exactly match what is being returned from the spatial engine.
        """
        geom = wkt.loads(data["geometry"])
        gdf = gpd.GeoDataFrame([data], geometry=[geom], crs=pyproj.CRS.from_user_input(crs))
        return gdf
    
    def on_point_draw(self, data):
        if data:
            x, y = data["x"][0], data["y"][0]

            view = gv.Points([(x, y)]).opts(size=10, color="green")
            plot.object = view

    pn.extension()
    hv.extension("bokeh")

    stac_href = "https://coclico.blob.core.windows.net/stac/v1/catalog.json"


    default_geometry = {
        "tr_name": "cl33475tr00223848",
        "lon": 4.27815580368042,
        "lat": 52.11359405517578,
        "bearing": 313.57275390625,
        "utm_crs": 32631,
        "coastline_name": 33475,
        "geometry": "LINESTRING (4.28855455531973 52.10728388554343, 4.267753743098557 52.119904391779215)",
        "bbox": {
            "maxx": 4.28855455531973,
            "maxy": 52.119904391779215,
            "minx": 4.267753743098557,
            "miny": 52.10728388554343,
        },
        "quadkey": "120201102230",
        "isoCountryCodeAlpha2": "NL",
        "admin_level_1_name": "Nederland",
        "isoSubCountryCode": "NL-ZH",
        "admin_level_2_name": "Zuid-Holland",
        "bounding_quadkey": "1202021102203",
    }

    default_geometry = prepare_default_geometry(default_geometry, crs=4326).to_crs(4326)

    point_draw = gv.Points([]).opts(size=10, color="red")
    app = pn.pane.HoloViews(default_geometry.hvplot(geo=True, line_color="red", line_width=1))
    point_draw_stream = streams.PointDraw(source=point_draw, num_objects=1)
    point_draw_stream.add_subscriber(on_point_draw)
    pn.Column(app.show()).servable()

    
