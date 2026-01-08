
from abc import ABC
from datetime import timedelta, datetime, tzinfo
from threading import Thread

import time
import durationpy
import os
import threading
import requests
from gateway import logger as base_logger


from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.background import BackgroundScheduler

from .event import BaseEventFabric


class _L:
        @staticmethod
        def info(*a, **k): print(*a)
        @staticmethod
        def warning(*a, **k): print(*a)
        
base_logger = _L()

def _post_create_model_event(payload=None, sch_env="SCH_SERVICE_NAME", timeout=5):
    sch = os.environ.get(sch_env, "http://scheduler:8000")
    url = sch.rstrip("/") + "/api/event"
    body = {"name": "CreateModelEvent", "data": payload or {"source": "monitoring"}}
    try:
        requests.post(url, json=body, timeout=timeout)
        base_logger.info("Posted CreateModelEvent to scheduler: %s", url)
    except Exception as e:
        base_logger.warning("Failed to post CreateModelEvent: %s", e)

def start_periodic_model_trigger(interval_minutes: int = 30, run_immediate: bool = True):
    def loop():
        if run_immediate:
            _post_create_model_event()
        while True:
            time.sleep(interval_minutes * 60)
            _post_create_model_event()
    t = threading.Thread(target=loop, daemon=True)
    t.start()
    return t

class Trigger(ABC):
    """
    Child objects of the :class:`Trigger <Trigger>` represent either One-Shot or
    Periodic Triggers for Event Request generation through the :class:`BaseEvent <event.BaseEvent>`
    factory.

    Once any child of this class has been instantiated, it will launch a local
    scheduler, which calls the given callback. The callback must take arguments.

    :param eventCallback: :class:`BaseEvent <event.BaseEvent>` instance to be called
    :param oneShot: indicates if the trigger must be run only once
    :param runImmediate: indicates the trigger should fire immediately
    :param cronSpec: indicates the periodicity of the trigger using Cron syntax
    """

    def __init__(self,
                 eventCallback: BaseEventFabric,
                 oneShot: bool,
                 runImmediate: bool = False,
                 cronSpec: str = "* * * * *"
                 ):
        super(Trigger, self).__init__()

        # Instantiate the BackgroundScheduler
        self.scheduler = BackgroundScheduler()
        self.job_identifier = None

        if runImmediate:
            next_time = datetime.now() + timedelta(seconds=30)
            self.scheduler.add_job(eventCallback,
                                   trigger="date",
                                   run_date=next_time,
                                   timezone="Europe/Berlin"
                                   )

        if oneShot and not runImmediate:
            self.job_identifier = self.scheduler.add_job(self.oneShotCallback(eventCallback),
                                                         CronTrigger.from_crontab(cronSpec, "Europe/Berlin"))

        if not oneShot:
            self.scheduler.add_job(eventCallback,
                                   CronTrigger.from_crontab(cronSpec,
                                                            "Europe/Berlin")
                                   )

        self.scheduler.start()

    def oneShotCallback(self, eventCallback: BaseEventFabric):
        def exec():
            eventCallback()
            self.scheduler.remove_job(self.job_identifier.id)
        return exec


class OneShotTrigger(Trigger):
    """
    Creates a One-Shot Trigger

    :param evt_cb: :class:`BaseEvent <event.BaseEvent>` instance to be called
    :param wait_time: indicates if there must be a delay before scheduling the first executions
    """

    def __init__(self, evt_cb: BaseEventFabric, runImmediate: bool = True, cronSpec: str = "* * * * *"):
        super(OneShotTrigger, self).__init__(
            evt_cb, oneShot=True, runImmediate=runImmediate, cronSpec=cronSpec)


class PeriodicTrigger(Trigger):
    """
    Creates a Periodic Trigger

    :param evt_cb: :class:`BaseEvent <event.BaseEvent>` instance to be called
    :param duration: frequency of event generation using Golang's time representation, e.g., 1h1m1s
    :param wait_time: indicates if there must be a delay before scheduling the first executions
    """

    def __init__(self, evt_cb: BaseEventFabric, runImmediate: bool = False, cronSpec: str = "* * * * *"):
        super(PeriodicTrigger, self).__init__(
            evt_cb, oneShot=False, runImmediate=runImmediate, cronSpec=cronSpec)



