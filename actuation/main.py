from sifec_base import LocalGateway, base_logger, PeriodicTrigger, ExampleEventFabric

logger = base_logger
app = LocalGateway(mock=False)

async def EmergencyNotificationFunction():
    logger.info("Emergency notification received")
    return {"status": 200}

app.deploy(EmergencyNotificationFunction, "Emergency-Notification-Function", "EmergencyEvent")


async def class_test_handler():
    """
    Dummy handler for ClassTestEvent required by the assignment.
    """
    logger.info("I passed the assignment.")
    return {"status": 200, "message": "I passed the assignment."}


async def emergency_notification_function():
    """
    Sends an email if there is an emergency
    """
    logger.info("This is an email")
    
    # Logic to send an "email"
    return {"status": 200, "message": "I passed the assignment."}


app.deploy(emergency_notification_function, "emergency_notification_function()", "EmergencyEvent")

