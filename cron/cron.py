from apscheduler.schedulers.background import BlockingScheduler, BaseScheduler
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

from settings import REDIS_HOST, REDIS_PORT

from pytz import utc

from tzlocal import get_localzone

import sys

sys.path.append('../')

import logging

from notifier import  start_covid_19_processor

log_tag = "convid_19_notifier"

redis_job_store = RedisJobStore(jobs_key="covid_19.jobs", run_times_key="covid_19.run_times", host=REDIS_HOST, port=REDIS_PORT)


jobstores = {
    'redis': redis_job_store
}

executors = {
    'default': ThreadPoolExecutor(20),
    'processpool': ProcessPoolExecutor(5)
}

job_defaults= {
    'coalesce': True,
    'max_instances': 1
}

timezone = get_localzone()

schedular = BlockingScheduler(jobstores=jobstores, executors=executors,job_defaults=job_defaults, timezone=timezone)

schedular.add_jobstore(redis_job_store)



@schedular.scheduled_job(id="start_covid_19_processor", name="start_covid_19_processor", trigger='interval', minutes=30, misfire_grace_time=None)
def run_covid_19_processor():

    logger = logging.getLogger(log_tag)
    logger.info("Starting job processing")

    try:
        start_covid_19_processor()
        print "Sync tak dispatcher"
    except:
        logger.info("Exception at processing onboarding", exc_info=True)

    logger.info("Create new spot instances")



