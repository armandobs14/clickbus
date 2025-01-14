DOCKER=docker compose -f minio.compose.yml -f spark.compose.yml -f compose.yml

# Setup commands
setup_minio:
	
	$(DOCKER) up -d mc

up:
	$(DOCKER) up -d spark spark-worker

pipeline:
	$(DOCKER) exec spark spark-submit \
			--packages io.delta:delta-core_2.12:2.3.0 \
			--py-files jobs/dist/functions.py,jobs/dist/etl.py,jobs/dist/writer.py,jobs/dist/event_processor.py,jobs/dist/aggregator.py \
			jobs/dist/pipeline.py
analytics:
	$(DOCKER) up -d jupyterlab

down:
	$(DOCKER) down