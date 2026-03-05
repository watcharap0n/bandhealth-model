.PHONY: help build up run run-skip-train run-hop run-hop-auto run-hop-chain shell clean logs

RUN_ID ?= local-run
HOP ?= load_tables

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Build the Docker image
	docker-compose build

up: ## Run the pipeline with docker-compose
	docker-compose up

run: ## Run the full pipeline with training
	docker-compose run --rm brand-health-pipeline \
		python run_pipeline.py \
		--dataset-root datasets \
		--reports-dir reports \
		--outputs-dir outputs \
		--artifacts-dir artifacts \
		--snapshot-freq 7D \
		--report-name brand_health_report.md

run-skip-train: ## Run pipeline without training (reuse model)
	docker-compose run --rm brand-health-pipeline \
		python run_pipeline.py \
		--dataset-root datasets \
		--reports-dir reports \
		--outputs-dir outputs \
		--artifacts-dir artifacts \
		--snapshot-freq 7D \
		--skip-train

run-v2: ## Run pipeline and generate V2 report
	docker-compose run --rm brand-health-pipeline \
		python run_pipeline.py \
		--dataset-root datasets \
		--reports-dir reports \
		--outputs-dir outputs \
		--artifacts-dir artifacts \
		--snapshot-freq 7D \
		--report-name band_health_report_V2.md

run-hop: ## Run one hop manually (set HOP=<hop>, RUN_ID=<run-id>)
	docker-compose run --rm brand-health-pipeline \
		python run_pipeline_hops.py $(HOP) \
		--run-id $(RUN_ID) \
		--dataset-root datasets \
		--reports-dir reports \
		--outputs-dir outputs \
		--artifacts-dir artifacts \
		--snapshot-freq 7D

run-hop-auto: ## Run one hop with --auto-upstream (set HOP=<hop>, RUN_ID=<run-id>)
	docker-compose run --rm brand-health-pipeline \
		python run_pipeline_hops.py $(HOP) \
		--run-id $(RUN_ID) \
		--auto-upstream \
		--dataset-root datasets \
		--reports-dir reports \
		--outputs-dir outputs \
		--artifacts-dir artifacts \
		--snapshot-freq 7D

run-hop-chain: ## Run full 9-hop chain in one container (set RUN_ID=<run-id>)
	docker-compose run --rm brand-health-pipeline sh -lc "\
		python run_pipeline_hops.py load_tables --run-id $(RUN_ID) --dataset-root datasets --reports-dir reports --outputs-dir outputs --artifacts-dir artifacts --snapshot-freq 7D && \
		python run_pipeline_hops.py join_diagnostics --run-id $(RUN_ID) --dataset-root datasets --reports-dir reports --outputs-dir outputs --artifacts-dir artifacts --snapshot-freq 7D && \
		python run_pipeline_hops.py profile --run-id $(RUN_ID) --dataset-root datasets --reports-dir reports --outputs-dir outputs --artifacts-dir artifacts --snapshot-freq 7D && \
		python run_pipeline_hops.py features --run-id $(RUN_ID) --dataset-root datasets --reports-dir reports --outputs-dir outputs --artifacts-dir artifacts --snapshot-freq 7D && \
		python run_pipeline_hops.py segments --run-id $(RUN_ID) --dataset-root datasets --reports-dir reports --outputs-dir outputs --artifacts-dir artifacts --snapshot-freq 7D && \
		python run_pipeline_hops.py labels --run-id $(RUN_ID) --dataset-root datasets --reports-dir reports --outputs-dir outputs --artifacts-dir artifacts --snapshot-freq 7D && \
		python run_pipeline_hops.py train --run-id $(RUN_ID) --dataset-root datasets --reports-dir reports --outputs-dir outputs --artifacts-dir artifacts --snapshot-freq 7D && \
		python run_pipeline_hops.py infer --run-id $(RUN_ID) --dataset-root datasets --reports-dir reports --outputs-dir outputs --artifacts-dir artifacts --snapshot-freq 7D && \
		python run_pipeline_hops.py publish --run-id $(RUN_ID) --dataset-root datasets --reports-dir reports --outputs-dir outputs --artifacts-dir artifacts --snapshot-freq 7D \
	"

shell: ## Open interactive shell in container
	docker-compose run --rm brand-health-pipeline /bin/bash

logs: ## Show container logs
	docker-compose logs -f

clean: ## Remove stopped containers and dangling images
	docker-compose down
	docker system prune -f

clean-all: ## Remove all containers, images, and volumes
	docker-compose down -v
	docker system prune -af
