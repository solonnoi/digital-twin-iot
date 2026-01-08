from sifec_base.trigger import OneShotTrigger
from sifec_base import LocalGateway, base_logger, PeriodicTrigger, ExampleEventFabric, TrainOccupancyModelEventFabric,CheckEmergencyEventFabric,EmergencyEventFabric


app = LocalGateway(mock=False)

# Add in a vacation mode. 


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

app.deploy(class_test_handler, "class-test-handler-monitoring", "ClassTestEvent")


async def emergency_handler():
    """
    This one just handles emergency checks periodically
    """
    # Logic to check for emergencies would go here
    #If emergency detected send EmergencyEvent
    evt3 = EmergencyEventFabric()
    tgr3 = OneShotTrigger(evt3, runImmediate=False)
    
    base_logger.info("This is an emergency")
    return {"status": 200, "message": "Emergency detected."}


app.deploy(emergency_handler, "emergency_handler()", "CheckEmergencyEvent")


evt = TrainOccupancyModelEventFabric()
tgr = PeriodicTrigger(evt, runImmediate=True, cronSpec="*/1 * * * *")

evt2 = CheckEmergencyEventFabric()
tgr2 = PeriodicTrigger(evt2, runImmediate=True, cronSpec="*/1 * * * *")

# This should trigger every 30 minutes and sends an event to create the occupancy model
# To the modeling component...



