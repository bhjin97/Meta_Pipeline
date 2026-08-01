# E-Commerce Hybrid Data Pipeline

> Kafka와 Spark를 기반으로 실시간 이벤트와 배치 데이터를 통합 처리하고,  
> Bronze–Silver–Gold 계층을 거쳐 분석용 데이터 마트로 제공하는 하이브리드 데이터 파이프라인입니다.

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

## 🏗 Architecture

<p align="center">
  <img src="./Data_Eng_Project.drawio (4).png"
       alt="E-Commerce Hybrid Data Pipeline Architecture"
       width="100%">
</p>

주문·배송·리뷰 이벤트를 Kafka로 수집하고 Spark Structured Streaming으로 실시간 처리합니다.  
원본 이벤트는 MinIO에 적재하며, 처리량과 누적 건수는 Redis와 Grafana를 통해 모니터링합니다.

Airflow가 Spark Batch 작업을 오케스트레이션하여 Silver·Gold 계층과 분석용 마트를 생성합니다.  
최종 데이터는 PostgreSQL에 적재하고 Metabase를 통해 시각화합니다.

## 📌 Project Overview

이커머스 환경에서 발생하는 주문·배송·리뷰 이벤트를 실시간으로 수집하고,  
정적 데이터와 결합해 분석용 데이터 마트까지 제공하는 데이터 파이프라인을 구축했습니다.


| 항목 | 내용 |
|---|---|
| 프로젝트 형태 | 개인 프로젝트 |
| 데이터셋 | Brazilian E-Commerce Public Dataset by Olist |
| 처리 방식 | 실시간 스트리밍 + 주기적 배치 |
| 실행 환경 | Oracle Cloud Compute Instance |
| 주요 데이터 | 주문·배송·리뷰 이벤트 및 고객·상품·판매자 데이터 |
| 제공 결과 | 분석용 데이터 마트 및 BI 대시보드 |

## 🎯 Key Achievements

| 핵심 성과 | 결과 |
|---|---|
| 하이브리드 파이프라인 구축 | Kafka 기반 실시간 처리와 Airflow로 자동화한 주기적 배치 처리를 하나의 데이터 흐름으로 통합 |
| 스트리밍 안정성 검증 | 100만 건의 이벤트를 최대 10,000 EPS로 입력하고 Kafka → Spark → MinIO 구간의 최종 적재 확인 |
| 대용량 배치 처리 검증 | 100만 건 규모의 데이터를 Silver·Gold 계층과 PostgreSQL 데이터 마트까지 정상 처리 |
| 운영 복구 구조 검증 | 스트리밍 중단 후 Kafka에 남은 이벤트를 체크포인트 기반으로 이어서 처리 |
| 분석 성능 개선 | PostgreSQL 인덱스 적용 전후의 조회 성능을 비교해 분석 쿼리 최적화 효과 확인 |
| 모니터링 및 장애 대응 | Redis·Grafana로 실시간 처리량을 시각화하고 Airflow 작업 실패 시 Slack 알림 전송 |

## 🧩 핵심 설계

### 이벤트 기반 데이터 모델링

정적인 Olist 데이터를 주문·배송·리뷰의 상태 변화를 나타내는 이벤트 스트림으로 재구성했습니다.  
이벤트 종류에 따라 Kafka 토픽을 분리하고 발생 시간순으로 전송하여 실시간 데이터 흐름을 재현했습니다.

### 스트리밍·배치 역할 분리

Spark Structured Streaming은 신규 이벤트 수집과 실시간 모니터링을, Spark Batch는 정적 데이터 결합과 Fact·Dimension·Mart 생성을 담당합니다.  
각 처리 방식의 역할을 분리해 실시간성과 복잡한 데이터 변환을 함께 지원하도록 구성했습니다.

### 재처리 가능한 계층형 저장 구조

원본 이벤트를 MinIO Bronze 계층에 보존하고, Silver와 Gold를 가공 계층으로 분리했습니다.  
작업 실패나 변환 로직 변경 시에도 원본 데이터를 기준으로 하위 계층을 다시 생성할 수 있습니다.

### 제한된 자원을 고려한 실행 전략

4 CPU·24GB RAM의 단일 VM에서 스트리밍과 배치의 자원 경쟁을 줄이기 위해 실행 시점을 분리했습니다.  
배치 중 수집된 이벤트는 Kafka에 보존하고, 스트리밍 재시작 후 체크포인트를 기준으로 이어서 처리합니다.
