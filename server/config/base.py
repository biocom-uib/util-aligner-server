import environs
from kombu import Queue

env = environs.Env()

CELERY_IMPORTS = ['main']

CELERY_BROKER_URL = env('CELERY_BROKER_URL',
                        "amqp://guest:guest@rabbitmq:5672//")
CELERY_ACCEPT_CONTENT = {'json'}
CELERY_TIMEZONE = 'Europe/Madrid'
CELERY_ENABLE_UTC = True

CELERY_WORKER_REDIRECT_STDOUTS = False
CELERY_WORKER_PREFETCH_MULTIPLIER = 4
CELERY_WORKER_MAX_TASKS_PER_CHILD = 500

CELERY_TASK_IGNORE_RESULT = True
CELERY_TASK_TIME_LIMIT = 7 * 60
CELERY_TASK_ACKS_LATE = True

CELERY_TASK_QUEUES = [
    Queue('server_default', routing_key='server_default',
          queue_arguments={'x-max-priority': 10})
]

CELERY_TASK_DEFAULT_QUEUE = 'server_default'
CELERY_TASK_DEFAULT_ROUTING_KEY = CELERY_TASK_DEFAULT_QUEUE
CELERY_TASK_DEFAULT_EXCHANGE = 'celery'
CELERY_TASK_DEFAULT_EXCHANGE_TYPE = 'direct'