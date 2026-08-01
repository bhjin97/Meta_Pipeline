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

실시간 이벤트는 Kafka와 Spark Structured Streaming으로 수집·적재하고,  
Airflow 기반 Spark Batch를 통해 Bronze–Silver–Gold 계층으로 정제합니다.  
최종 데이터는 PostgreSQL과 Metabase를 통해 분석에 활용하며,  
Grafana 모니터링과 Slack 실패 알림을 통해 운영 상태를 확인할 수 있도록 구성했습니다.

| 항목 | 내용 |
|---|---|
| 프로젝트 형태 | 개인 프로젝트 |
| 데이터셋 | Brazilian E-Commerce Public Dataset by Olist |
| 처리 방식 | 실시간 스트리밍 + 주기적 배치 |
| 실행 환경 | Oracle Cloud Compute Instance |
| 주요 데이터 | 주문·배송·리뷰 이벤트 및 고객·상품·판매자 데이터 |
| 제공 결과 | 분석용 데이터 마트 및 BI 대시보드 |
