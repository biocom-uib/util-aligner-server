celery-default: queue = server_default
purge-celery-default: queue = server_default

celery-queue:
	docker-compose run --rm server celery -A server_queue worker -Q $(queue) -l info

purge-celery-queue:
	docker-compose run --rm server celery -A server_queue purge -Q $(queue)

celery-default: celery-queue

purge-celery-default: purge-celery-queue