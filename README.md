# 🛒 E-Commerce Hybrid Data Pipeline Project
## 아키텍처 구조
![Architecture](./Data_Eng_Project.drawio.png)  
        

---

## 1. 📌 프로젝트 개요

본 프로젝트는 정적인 CSV 기반 이커머스 데이터를 **이벤트 기반 스트리밍 데이터 구조로 변환**하고,  
Kafka를 중심으로 데이터 파이프라인을 구축하는 것을 목표로 한다.

Olist 이커머스 데이터를 활용하여 주문, 배송, 리뷰 데이터를 이벤트 형태로 생성하고,  
이를 Kafka를 통해 전달한 후 Consumer를 통해 저장하는 구조를 구현하였다.

### 🎯 주요 목표

- 배치 데이터를 이벤트 스트림 형태로 변환
- Kafka 기반 데이터 수집 파이프라인 구축
- 실시간 데이터 처리 구조 설계
- 이후 Spark 및 BI 분석을 위한 데이터 기반 마련

---

## 2. 🗂️ 프로젝트 구조

```text
Meta_Pipeline/
├── data/
│   ├── raw/                 # 원본 Olist 데이터
│   ├── event_source/        # 이벤트 생성 데이터 (JSONL)
│   ├── sink/                # Kafka Consumer 저장 결과
│
├── kafka/
│   ├── docker-compose.yml   # Kafka, Zookeeper, UI 설정
│   ├── producer/            # Kafka Producer
│   ├── consumer/            # Kafka Consumer (파일 저장)
│
├── origin_data_processing/
│   └── jobs/                # 이벤트 생성 스크립트
│
└── README.md
