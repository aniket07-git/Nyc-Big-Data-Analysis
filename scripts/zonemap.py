import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
from descartes.patch import PolygonPatch
from shapely.geometry import Polygon
import shapefile  # PyShp module
from query import PUtop3_local
from query import DOtop3_local

# Load your shapefile
shp_base_path = r"/Users/yuvrajpatadia/Downloads/NYC Taxi Zones/geo_export_792ecb87-baae-4c22-a88d-a2d9b25b77c4"
sf = shapefile.Reader(shp_base_path)
shp_dic = {field[0]: n for n, field in enumerate(sf.fields)}

# Your draw_zone_map function here
def draw_zone_map(ax, sf, heat={}, text=[], arrows=[]):
    continent = [235/256, 151/256, 78/256]  # color for the land
    ocean = (89/256, 171/256, 227/256)  # color for the ocean
    theta = np.linspace(0, 2*np.pi, len(text)+1).tolist()
    ax.set_facecolor(ocean)

    # setting up colorbar (if heat map data provided)
    if len(heat) != 0:
        norm = mpl.colors.Normalize(vmin=min(heat.values()), vmax=max(heat.values()))
        cm = plt.get_cmap('Reds')
        sm = plt.cm.ScalarMappable(cmap=cm, norm=norm)
        sm.set_array([])
        cbar = plt.colorbar(sm, ax=ax, ticks=np.linspace(min(heat.values()), max(heat.values()), 8), 
                            boundaries=np.arange(min(heat.values())-10, max(heat.values())+10, .1))
    
    # drawing the zones
    for sr in sf.shapeRecords():
        shape = sr.shape
        rec = sr.record
        loc_id = rec[5]  # Assuming the first field in your shapefile record is LocationID


        if len(heat) == 0:
            col = continent
        else:
            col = cm(norm(heat.get(loc_id, 0)))  # color based on heat value for loc_id


        if not shape.points or not isinstance(shape.points[0], (list, tuple)):
            continue  # Skip if points are not in the expected format

        nparts = len(shape.parts)  # Number of parts
        for ip in range(nparts):
            i0 = shape.parts[ip]
            i1 = shape.parts[ip + 1] if ip < nparts - 1 else len(shape.points)
            polygon_points = shape.points[i0:i1]

            # Error handling for Polygon creation
            try:
                polygon = Polygon(polygon_points)
                patch = PolygonPatch(polygon, facecolor=col, edgecolor='k', alpha=1.0, zorder=2)
                ax.add_patch(patch)
                if polygon.exterior:  # Check if polygon has exterior
                    x, y = np.array(polygon.exterior.coords.xy).mean(axis=1)

                    # Annotation
                    if polygon.exterior:  # Check if polygon has exterior
                        x, y = np.array(polygon.exterior.coords.xy).mean(axis=1)
                        if loc_id in text:
                            eta_x, eta_y = 0.05 * np.cos(theta[text.index(loc_id)]), 0.05 * np.sin(theta[text.index(loc_id)])
                            ax.annotate("[{}]".format(loc_id), xy=(x, y), xytext=(x+eta_x, y+eta_y),
                                bbox=dict(facecolor='black', alpha=0.5), color="white", fontsize=12,
                                arrowprops=dict(facecolor='black', width=3, shrink=0.05))
                    
            except Exception as e:
                print(f"Error processing polygon with points: {polygon_points}")
                print(f"Error: {e}")
                continue
# Prepare data for heat map (example, adjust according to your needs)
heat_map_data = {row['LocationID']: row['PUcount'] for _, row in PUtop3_local.iterrows()}

# Visualization
fig, ax = plt.subplots(nrows=1, ncols=2, figsize=(18,8))

# Plot for top 3 pickup locations
ax1 = plt.subplot(1, 2, 1)
ax1.set_title("Zones with most pickups")
draw_zone_map(ax1, sf, heat=heat_map_data)

# Plot for top 3 dropoff locations
ax2 = plt.subplot(1, 2, 2)
ax2.set_title("Zones with most drop-offs")
draw_zone_map(ax2, sf, heat=heat_map_data)  # Adjust this according to your dropoff data

plt.show()
