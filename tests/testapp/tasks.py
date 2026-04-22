import logging

from django.tasks import task

logger = logging.getLogger(__name__)


@task
def my_task():
    logger.info("Hello World!")
