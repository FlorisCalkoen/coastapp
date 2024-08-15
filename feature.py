import panel as pn
import pyperclip


class FeatureManager:
    def __init__(self, spatial_query_app):
        """
        Initializes the FeatureManager with a reference to the SpatialQueryApp to access 
        current transect data (lon, lat, transect_id).
        
        Args:
            spatial_query_app: Instance of SpatialQueryApp that holds the selected transect data.
        """
        self.spatial_query_app = spatial_query_app

        # Panel widgets
        self.google_maps_url = pn.pane.Markdown("")
        self.copy_coords_button = pn.widgets.Button(
            name="Copy Coords to Clipboard (Google Earth format)", button_type="default"
        )
        self.copy_transect_id_button = pn.widgets.Button(
            name="Copy Transect ID to Clipboard", button_type="default"
        )

        # Set up callbacks
        self.copy_coords_button.on_click(self.copy_coords_to_clipboard)
        self.copy_transect_id_button.on_click(self.copy_transect_id_to_clipboard)

        # Update the Google Maps URL whenever the transect is updated
        self.spatial_query_app.param.watch(self.update_google_maps_url, "current_transect")

        # Initial update of the Google Maps URL
        self.update_google_maps_url()

    def update_google_maps_url(self, event=None):
        """
        Update the Google Maps URL link based on the current transect's coordinates.
        """
        selected_geometry = self.spatial_query_app.get_selected_geometry()
        if selected_geometry['lon'] and selected_geometry['lat']:
            lon, lat = selected_geometry['lon'], selected_geometry['lat']
            zoom = 18  # You can adjust this zoom level if necessary
            url = f"https://www.google.com/maps/@{lat},{lon},{zoom}z"
            link = f'<a href="{url}" target="_blank">Open in Google Maps (street view) </a>'
            self.google_maps_url.object = link
        else:
            self.google_maps_url.object = "No location data available."

    def copy_coords_to_clipboard(self, event):
        """
        Copy the lon/lat coordinates to the clipboard in Google Maps format.
        """
        selected_geometry = self.spatial_query_app.get_selected_geometry()
        if selected_geometry['lon'] and selected_geometry['lat']:
            lat_lon_str = f"{selected_geometry['lat']}, {selected_geometry['lon']}"
            pyperclip.copy(lat_lon_str)  # Copies to clipboard
            pn.state.notifications.success(f"Copied: {lat_lon_str}")
        else:
            pn.state.notifications.error("No location data available to copy.")

    def copy_transect_id_to_clipboard(self, event):
        """
        Copy the transect ID to the clipboard.
        """
        selected_geometry = self.spatial_query_app.get_selected_geometry()
        transect_id = selected_geometry.get('transect_id')
        if transect_id:
            pyperclip.copy(transect_id)  # Copies to clipboard
            pn.state.notifications.success(f"Copied Transect ID: {transect_id}")
        else:
            pn.state.notifications.error("No Transect ID available to copy.")

    def view(self):
        """
        View for displaying the FeatureManager interface.
        """
        return pn.Column(
            self.google_maps_url,
            self.copy_coords_button,
            self.copy_transect_id_button,
            name="Feature Management",
        )
