from flask import Flask, request
import folium
import os
from folium.plugins import MarkerCluster
from databricks import sdk, sql

app = Flask(__name__)


def query(request):
    email = request.headers.get("x-forwarded-email")
    # token = request.headers.get("x-forwarded-access-token")
    cfg = sdk.config.Config()
    print("using SP auth")
    with sql.connect(
        server_hostname=cfg.host,
        http_path=os.getenv("HTTP_PATH"),
        credentials_provider=lambda: cfg.authenticate,
        # access_token=token,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM jgcatalog.geo.price_paid_diffs_with_coordinates_view"
            )
            data = cursor.fetchall_arrow().to_pandas()
            return data


def create_map(filtered_df):
    # Initialize a map centered at a default location
    map_center = [51.5074, -0.1278]  # Default center (London)
    map_ = folium.Map(location=map_center, zoom_start=10)

    # Use MarkerCluster to group nearby points
    marker_cluster = MarkerCluster().add_to(map_)

    # Add filtered data as markers
    for _, row in filtered_df.iterrows():
        address = row["Address"]
        price = row["Latest_sale_price_paid"]
        lat = row["latitude"]
        lon = row["longitude"]

        # Construct the URL for the Street View image
        street_view_url = f"https://maps.googleapis.com/maps/api/streetview?size=600x300&location={lat},{lon}&key=<<fill in>>"

        popup_text = f"""
        <b><h4>{row['Address']} ({row['latitude']},{row['longitude']})</h4></b><br/>
        <img src="{street_view_url}" alt="Street View" style="width:100%; height:auto;"><br/>
        <br/>
        <b>Latest sale</b><br/>
        Date: {row['Latest_sale_date_of_transfer']}<br/>
        Price paid: £{row['Latest_sale_price_paid']}<br/>
        HPI: {row['hpi_index']}<br/>
        <br/>
        <b>Earliest Sale</b><br/>
        Date: {row['Earliest_sale_date_of_transfer']}<br/>
        Price paid: £{row['Earliest_sale_price_paid']}<br/>
        HPI: {row['earliest_sale_hpi_index']}<br/>
        <br/>
        <b>Calculations</b>:<br/>
        price_diff_latest_to_earliest_sale: £{row['price_diff_latest_to_earliest_sale']}<br/>
        expected_price_given_hpi: £{row['expected_price_given_hpi']}<br/>
        diff_actual_price_vs_expected: £{row['diff_actual_price_vs_expected']}<br/>
        percentage_diff_actual_from_expected: {row['percentage_diff_actual_from_expected']}
        <br/>
        <div style="display: flex; gap: 10px; margin-top: 10px;">
            <a target="_blank" href="http://maps.google.com/maps?q=&layer=c&cbll={row['latitude']},{row['longitude']}" style="text-decoration:none;">
                <button style="background-color:#4CAF50; color:white; padding:10px 20px; border:none; border-radius:5px; cursor:pointer;">Street view</button>
            </a>
            <a target="_blank" href="https://idoxpa.westminster.gov.uk/online-applications/propertyDetails.do?activeTab=relatedCases&keyVal=002097RPLI000" style="text-decoration:none;">
                <button style="background-color:#008CBA; color:white; padding:10px 20px; border:none; border-radius:5px; cursor:pointer;">Planning</button>
            </a>
            <a target="_blank" href="https://www.rightmove.co.uk/house-prices/details/england-43232971-17999917?s=20540741cca129341b2c1f9fb069f197785a1f9d11f63c51093ee6918d182284#/" style="text-decoration:none;">
                <button style="background-color:#FFA500; color:white; padding:10px 20px; border:none; border-radius:5px; cursor:pointer;">Advert</button>
            </a>
            
        </div>
        """
        folium.Marker(location=[lat, lon], popup=popup_text).add_to(marker_cluster)

    map_html = map_.get_root().render()
    return map_html


@app.route("/")
def index():
    query_results = query(request)
    map_ = create_map(query_results)
    return map_


if __name__ == "__main__":
    app.run(debug=True)
