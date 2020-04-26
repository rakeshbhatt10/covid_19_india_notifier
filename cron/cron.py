from apscheduler.schedulers.background import BlockingScheduler, BaseScheduler
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

from settings import REDIS_HOST, REDIS_PORT

from pytz import utc

from tzlocal import get_localzone

import sys

sys.path.append('../')


from onboarding import publish_job_to_spot_instances, create_new_spot_instances, start_onboarding_dispatcher, start_updater, start_synctask_dispatcher, start_client_dispacher, start_ota_dispatcher, start_flyer_reviews_updater

from logger import get_logger, format

log_tag = "service_onboarding"

redis_job_store = RedisJobStore(jobs_key="service_onboarding.jobs", run_times_key="service_onboarding.run_times", host=REDIS_HOST, port=REDIS_PORT)


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
# schedular.add_executor(ThreadPoolExecutor(20), alias="default")
# schedular.add_executor(ThreadPoolExecutor(5), alias="processpool")

# @schedular.scheduled_job(id="sftp_email_archive", name="start_pms_schedular", trigger='cron', day_of_week="0-7", hour="15-16,22-23")
# def start_pms_schedular():
#     logger = get_logger(log_tag)
#     logger.info("Starting log processing")
#     try:
#         start_sftp_email_archive()
#     except:
#         logger.info("Exception at processing pms schedular", exc_info=True)
#
#     logger.info("All pms issues parsed are processed")




# @schedular.scheduled_job(id="onboarding", name="start_onboarding", trigger='interval', minutes=8)
# def start_onboarding_schedular():
#     logger = get_logger(log_tag)
#     logger.info("Starting log processing")
#     try:
#         start_onboarding_dispatcher()
#     except:
#         logger.info("Exception at processing onboarding", exc_info=True)
#
#     logger.info("All pms issues parsed are processed")


@schedular.scheduled_job(id="run_spot_instances", name="run_spot_instances", trigger='cron', day_of_week="0-7", hour="7,19", misfire_grace_time=None)
def run_spot_instances():

    logger = get_logger(log_tag)
    logger.info("Starting log processing")

    try:
        create_new_spot_instances()
        print "Sync tak dispatcher"
    except:
        logger.info("Exception at processing onboarding", exc_info=True)

    logger.info("Create new spot instances")

@schedular.scheduled_job(id="publish_job_to_spot_instances", name="publish_job_to_spot_instances", trigger='interval', minutes=7, misfire_grace_time=None)
def run_job_to_spot_instances():

    logger = get_logger(log_tag)
    logger.info("Starting log processing")

    try:
        publish_job_to_spot_instances()
        print "Sync tak dispatcher"
    except:
        logger.info("Exception at processing onboarding", exc_info=True)

    logger.info("Create new spot instances")



@schedular.scheduled_job(id="synctask", name="synctask", trigger='cron', day_of_week="0-7", hour="7,16,21", misfire_grace_time=None)
def schedule_sync_task():

    logger = get_logger(log_tag)
    logger.info("Starting log processing")

    try:
        start_synctask_dispatcher()
        print "Sync tak dispatcher"
    except:
        logger.info("Exception at processing onboarding", exc_info=True)

    logger.info("Sync task applied")


@schedular.scheduled_job(id="clienttask", name="clienttask", trigger='interval', hours=5, misfire_grace_time=None)
def scedule_client_sync_task():

    logger = get_logger(log_tag)
    logger.info("Starting log processing")

    try:
        # start_client_dispacher(True)
        print "clienttask dispatcher"
    except:
        logger.info("Exception at processing onboarding", exc_info=True)

    logger.info("clienttask applied")
#
#
# @schedular.scheduled_job(id="hiqsync", name="hiqsync", trigger='cron', day_of_week="0-7", hour="21,22")
# def schedule_holidayiq_sync_task():
#
#     logger = get_logger(log_tag)
#     logger.info("Starting log processing")
#
#     try:
#         start_ota_dispatcher("holidayiq")
#     except:
#         logger.info("Exception at processing onboarding", exc_info=True)
#
#     logger.info("Hiq Sync task applied")


@schedular.scheduled_job(id="updater", name="updater", trigger='interval', minutes=7)
def start_updator_schedular():

    logger = get_logger(log_tag)
    logger.info("Starting log processing")

    try:
        # start_onboarding_dispatcher()
        start_updater()
    except:
        logger.info("Exception at processing updater", exc_info=True)

    logger.info("Updater")


@schedular.scheduled_job(id="flyer_reviews_updater", name="flyer_reviews_updater", trigger='cron', day_of_week="0-7", hour="7,16,21", misfire_grace_time=None)
def flyer_reviews_updater():

    logger = get_logger(log_tag)
    logger.info("Starting log processing")

    try:
        start_flyer_reviews_updater()
    except:
        logger.info("Exception at processing updater", exc_info=True)

    logger.info("Updater")

