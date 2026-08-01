# E-Commerce Hybrid Data Pipeline

> Kafka와 Spark를 기반으로 실시간 이벤트와 배치 데이터를 통합 처리하고,  
> Bronze–Silver–Gold 계층을 거쳐 분석용 데이터 마트로 제공하는 하이브리드 데이터 파이프라인입니다.

## 🏗 Architecture

<p align="center">
  <img src="./Data_Eng_Project.drawio (4).png"
       alt="E-Commerce Hybrid Data Pipeline Architecture"
       width="100%">
</p>

주문·배송·리뷰 이벤트를 Kafka로 수집하고 Spark Structured Streaming으로 실시간 처리합니다.  
원본 이벤트는 MinIO에 보존하며, 처리량과 누적 건수는 Redis와 Grafana로 모니터링합니다.

Airflow가 주기적인 Spark Batch 작업을 오케스트레이션하여 Silver·Gold 계층과 분석용 데이터 마트를 생성합니다.  
최종 데이터는 PostgreSQL에 적재하고 Metabase를 통해 시각화합니다.

---

## 📌 Project Overview

정적인 Olist 데이터셋을 주문·배송·리뷰 이벤트로 재구성하고,  
실시간 수집부터 데이터 정제, 마트 생성, BI 시각화까지 연결되는 파이프라인을 구축했습니다.

- **프로젝트 형태:** 개인 프로젝트
- **데이터셋:** Brazilian E-Commerce Public Dataset by Olist
- **처리 방식:** 실시간 스트리밍 + 주기적 배치
- **실행 환경:** Oracle Cloud 단일 VM `(4 CPU · 24GB RAM)`
- **주요 데이터:** 주문·배송·리뷰 이벤트, 고객·상품·판매자 데이터
- **최종 결과:** 분석용 데이터 마트, BI 대시보드, 운영 모니터링 환경

---

## 🎯 Key Achievements

| 핵심 성과 | 검증 결과 |
|---|---|
| 하이브리드 파이프라인 구축 | Kafka 기반 실시간 처리와 Airflow로 자동화한 주기적 배치 처리를 하나의 데이터 흐름으로 통합 |
| 스트리밍 안정성 검증 | 100만 건의 이벤트를 최대 10,000 EPS로 입력하고 Kafka → Spark → MinIO 구간의 최종 적재 확인 |
| 대량 배치 처리 검증 | 100만 건 규모의 데이터를 Silver·Gold 계층과 PostgreSQL 데이터 마트까지 정상 처리 |
| 운영 복구 구조 검증 | 스트리밍 중단 중 Kafka에 보존된 이벤트를 재시작 후 체크포인트를 기준으로 이어서 처리 |
| 분석 성능 개선 | PostgreSQL 인덱스 적용 전후의 조회 성능을 비교하여 분석 쿼리 최적화 효과 확인 |
| 모니터링 및 장애 대응 | Redis·Grafana로 실시간 처리량을 시각화하고 Airflow 작업 실패 시 Slack 알림 전송 |

---

## 🧩 Core Design

### 이벤트 기반 데이터 모델링

정적인 Olist 데이터를 주문·배송·리뷰의 상태 변화를 나타내는 이벤트 스트림으로 재구성했습니다.  
이벤트 유형에 따라 Kafka 토픽을 분리하고 발생 시간순으로 전송하여 실시간 데이터 흐름을 재현했습니다.

### 스트리밍과 배치의 역할 분리

Spark Structured Streaming은 신규 이벤트 수집과 실시간 모니터링을 담당합니다.  
Spark Batch는 정적 데이터 결합과 Fact·Dimension·Mart 생성을 담당하도록 역할을 분리했습니다.

이를 통해 실시간 수집과 복잡한 데이터 변환을 하나의 파이프라인에서 함께 지원합니다.

### 재처리 가능한 계층형 저장 구조

원본 이벤트를 MinIO의 Bronze 계층에 보존하고 Silver와 Gold를 가공 계층으로 분리했습니다.  
작업 실패나 변환 로직 변경 시에도 Bronze 데이터를 기준으로 하위 계층을 다시 생성할 수 있습니다.

### 제한된 자원을 고려한 실행 전략

4 CPU·24GB RAM의 단일 VM에서 스트리밍과 배치의 자원 경쟁을 줄이기 위해 실행 시점을 분리했습니다.  
배치 중 수집된 이벤트는 Kafka에 보존하고, 스트리밍 재시작 후 체크포인트를 기준으로 이어서 처리합니다.

---

## 🛠 Tech Stack

### Data Processing & Orchestration

![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)

### Storage & Database

![MinIO](https://img.shields.io/badge/MinIO-C72E49?style=flat-square&logo=minio&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=flat-square&logo=postgresql&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-FF4438?style=flat-square&logo=redis&logoColor=white)

### Analytics & Monitoring

![Metabase](https://img.shields.io/badge/Metabase-509EE3?style=flat-square&logo=metabase&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=flat-square&logo=grafana&logoColor=white)
![Slack](https://img.shields.io/badge/Slack-4A154B?style=flat-square&logo=slack&logoColor=white)

### Infrastructure

![Oracle Cloud](https://img.shields.io/badge/Oracle_Cloud-F80000?style=flat-square&logo=oracle&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)
![Ubuntu](https://img.shields.io/badge/Ubuntu-E95420?style=flat-square&logo=ubuntu&logoColor=white)

---

## 🗂 데이터 계층

데이터를 가공 수준과 사용 목적에 따라 Bronze–Silver–Gold 계층으로 분리했습니다.

`Kafka Event + Olist Data → Bronze → Silver → Gold → PostgreSQL → Metabase`

| 계층 | 역할 | 주요 처리 |
|---|---|---|
| **Bronze** | 원본 데이터 보존 | Kafka 이벤트와 Olist 정적 데이터를 Parquet 형식으로 저장 |
| **Silver** | 분석 가능한 표준 데이터 생성 | 데이터 정제·중복 제거·조인을 통해 Fact·Dimension 테이블 생성 |
| **Gold** | 분석 목적별 데이터 집계 | 매출·배송·리뷰·고객 세그먼트 기반 데이터 마트 생성 |

- **Data Lake:** MinIO의 Bronze → Silver → Gold
- **Serving:** PostgreSQL을 통한 SQL 조회 및 Metabase 대시보드 제공

---
## 🔧 대표 트러블슈팅

### 스트리밍 컨테이너 반복 재시작과 데이터 중복

**문제**

여러 개의 Structured Streaming Query 중 하나가 종료되면 스트리밍 프로세스 전체가 종료되고, Docker의 재시작 정책에 의해 컨테이너가 반복적으로 재실행되었습니다. 이 과정에서 Grafana에 동일 컨테이너의 시계열이 여러 개 나타나고 Redis 처리량이 비정상적으로 누적되는 현상이 발생했습니다.

**원인**

`awaitAnyTermination()` 구조에서는 실행 중인 Query 하나가 종료될 때 대기 상태가 해제되면서 애플리케이션이 종료되었습니다. 이후 컨테이너가 재시작되었고, 체크포인트를 초기화한 상태에서는 Kafka에 남아 있던 메시지를 `earliest`부터 다시 처리하면서 기존 이벤트가 중복 집계되었습니다.

**해결**

각 Streaming Query에 `queryName`을 부여하고 Query별 종료 대기 구조로 변경하여, 특정 Query의 종료가 전체 프로세스의 반복 재시작으로 이어지지 않도록 수정했습니다. 운영·테스트 토픽과 체크포인트도 분리하고 기존 체크포인트를 정리한 뒤 데이터 흐름을 다시 검증했습니다.

**결과**

컨테이너의 반복 재시작이 중단되었으며 Redis·Grafana의 중복 집계와 Prometheus의 불필요한 다중 시계열 발생을 방지했습니다.

---

### 스트리밍과 배치의 리소스 경쟁

**문제**

4 CPU·24GB RAM의 단일 VM에서 Spark Streaming과 Spark Batch를 동시에 실행하면 CPU와 메모리 사용량이 증가하고 배치 작업이 대기하거나 실패할 가능성이 발생했습니다.

**원인**

지속적으로 동작하는 스트리밍 작업과 대규모 조인·집계를 수행하는 배치 작업이 동일한 Spark Worker 자원을 동시에 사용했습니다.

**해결**

Airflow DAG에서 배치 실행 전 스트리밍을 중지하고, 배치 완료 후 다시 시작하도록 실행 순서를 변경했습니다. 중단 중 유입되는 이벤트는 Kafka에 보존하고, 재시작 후 Spark 체크포인트를 기준으로 미처리 이벤트를 이어서 처리하도록 구성했습니다.

**결과**

스트리밍과 배치의 자원 경쟁을 줄이면서 배치 작업을 안정적으로 완료했고, 스트리밍 중단 구간에 발생한 이벤트도 재시작 후 정상적으로 처리되는 것을 확인했습니다.
