SHELL := /bin/bash

LOAD_TEST_ID ?= load_test_1000000
EPS ?= 2500
TOPIC_PREFIX ?= load-test-

.PHONY: help load-reset load-reset-safe load-run

help:
	@echo "부하 테스트 명령"
	@echo ""
	@echo "  make load-reset"
	@echo "      확인 없이 부하 테스트 환경을 초기화합니다."
	@echo ""
	@echo "  make load-reset-safe"
	@echo "      yes 확인 후 부하 테스트 환경을 초기화합니다."
	@echo ""
	@echo "  make load-run"
	@echo "      Producer를 실행합니다."
	@echo ""
	@echo "기본 변수"
	@echo "  LOAD_TEST_ID=$(LOAD_TEST_ID)"
	@echo "  EPS=$(EPS)"
	@echo "  TOPIC_PREFIX=$(TOPIC_PREFIX)"
	@echo ""
	@echo "실행 예시"
	@echo "  make load-reset"
	@echo "  make load-reset-safe"
	@echo "  make load-run EPS=2500"
	@echo "  make load-run EPS=700"
	@echo "  make load-run LOAD_TEST_ID=load_test_500000 EPS=1000"
	@echo "  make load-test EPS=2500"
	@echo "      초기화 후 Producer까지 연속 실행합니다."

load-reset:
	@FORCE=1 ./scripts/load_test/reset.sh "$(LOAD_TEST_ID)"

load-reset-safe:
	@./scripts/load_test/reset.sh "$(LOAD_TEST_ID)"

load-run:
	@./scripts/load_test/run.sh \
		"$(LOAD_TEST_ID)" \
		"$(EPS)" \
		"$(TOPIC_PREFIX)"

.PHONY: load-test

load-test: load-reset
	@echo ""
	@echo "초기화가 완료되었습니다. Producer를 실행합니다."
	@./scripts/load_test/run.sh \
		"$(LOAD_TEST_ID)" \
		"$(EPS)" \
		"$(TOPIC_PREFIX)"
