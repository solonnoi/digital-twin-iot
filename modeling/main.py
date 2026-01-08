from sifec_base import LocalGateway, base_logger, PeriodicTrigger, ExampleEventFabric, ModelEventFabric
import os
import json
import io
from minio import Minio
from influxdb_client import InfluxDBClient


app = LocalGateway()


async def demo():
    base_logger.info("HELLO WORLD!!! You did it! :D")
    return

async def base_fn():
    """
    Test example of dynamically deploying another route upon an HTTP request

    Since this function will be invoked once the `test` event is triggered,
    the route `/api/other` will be registered at runtime rather than upon 
    starting the server. Such behavior allows to dynamically create functions
    that could answer to new events. Be aware that registering two functions
    with the same name will result in only one route. You can change this
    behavior by providing the `path` argument.

    Once this new route is registered, you will see it in the Homecare Hub under
    SIF Status, which means upon receiving an event (in this case `test`), you
    will see in the logs of this example the print above.
    """
    app.deploy(demo, "demo-fn", "GenEvent")
    return {"status": 200}

# Deploy a route within this server to be reachable from the SIF scheduler
# it appends the name of the cb to `/api/`. For more, please read the
# documentation for `deploy`
app.deploy(base_fn, "fn-fabric", "CreateFn")

async def class_test_handler():
    """
    Dummy handler for ClassTestEvent required by the assignment.
    """
    base_logger.info("I passed the assignment.")
    return {"status": 200, "message": "I passed the assignment."}

app.deploy(class_test_handler, "class-test-handler", "ClassTestEvent")


# Adding in a connection to minioooo


def get_minio_client():
    endpoint = os.environ["MINIO_ENDPOINT"]
    access_key = os.environ["MINIO_ACCESS_KEY"]
    secret_key = os.environ["MINIO_SECRET_KEY"] 

    secure = endpoint.startswith("https://")
    endpoint = endpoint.replace("http://", "").replace("https://", "")

    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )


def get_influx_client():
    url = os.environ["INFLUX_URL"]
    token = os.environ["INFLUX_TOKEN"]
    org = os.environ["INFLUX_ORG"]
    return InfluxDBClient(url=url, token=token, org=org)

# TODO Make this more complex and well actually useful
async def create_model_from_influx():
    """
    simple model:
    - Reads hourly coarsened activity from Influx (last 24h)
    - Computes simple statistics: mean and max hourly activity
    - Stores the result as a JSON model in MinIO
    """

    bucket = os.environ["INFLUX_BUCKET"]          # "activities"
    org = os.environ["INFLUX_ORG"]
    room_label = os.environ.get("MODEL_ROOM", "Bedroom")

    influx = get_influx_client()
    query_api = influx.query_api()

    # Minimal Flux query: 24 hours of activity counts (coarsened)
    flux_query = f'''
from(bucket: "{bucket}")
  |> range(start: -7d)
  |> filter(fn: (r) =>
      r._measurement == "activity" and
      r._field == "duration" and
      r.type == "bedroom" and
      r.source_bucket == "4_2_3"
  )
  |> aggregateWindow(every: 1h, fn: count, createEmpty: false)
  |> keep(columns: ["_time", "_value"])
'''

    tables = query_api.query(org=org, query=flux_query)
    values = [record.get_value() for table in tables for record in table.records]

    if not values:
        base_logger.warning("No activity data available to build model.")
        return {"status": 404, "message": "No data available"}

    # Minimal statistics
    mean_val = sum(values) / len(values)
    max_val = max(values)

    model = {
        "room": room_label,
        "bucket": bucket,
        "feature": "hourly_activity",
        "hours_sampled": len(values),
        "mean_hourly_activity": mean_val,
        "max_hourly_activity": max_val,
        "description": "Simple baseline model derived from last 7d of coarsened activity data."
    }

    # ---- Store in MinIO ----
    client = get_minio_client()
    bucket_name = os.environ.get("MINIO_BUCKET", "dt-models")

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    obj_name = "models/activity_baseline_model.json"
    data = json.dumps(model, indent=2).encode("utf-8")

    client.put_object(
        bucket_name,
        obj_name,
        io.BytesIO(data),
        length=len(data),
        content_type="application/json",
    )

    base_logger.info("Stored baseline model to MinIO: %s/%s", bucket_name, obj_name)

    return {
        "status": 200,
        "message": f"Model stored as {bucket_name}/{obj_name}",
        "model": model,
    }


# Expose it as a SIF function / HTTP endpoint
app.deploy(create_model_from_influx, "create_model_from_influx", "CreateModelEvent")


# evt = ExampleEventFabric()



evt = ModelEventFabric()

# tgr = PeriodicTrigger(evt, runImmediate=True)
# This is the "CreateOccupancyModelFunction"
tgr2 = PeriodicTrigger(evt, runImmediate=True, cronSpec="*/30 * * * *")