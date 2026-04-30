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
```
### 2. 🗂️ 최종 프로젝트 구조(예정)

```text
Meta_Pipeline/
├── docker-compose.yml              # 전체 컨테이너 통합 실행 설정
├── README.md
├── requirements.txt
├── .env                            # 환경변수 설정
├── .gitignore
│
├── data/
│   ├── raw/                        # 원본 Olist CSV 데이터
│   ├── event_source/               # Kafka Producer 입력용 이벤트 JSONL
│   ├── sink/                       # Kafka Consumer 저장 결과
│   ├── checkpoint/                 # Spark Streaming checkpoint
│   └── warehouse/                  # Spark 처리 결과 저장 영역
│
├── origin_data_processing/
│   └── jobs/                       # 원본 CSV → 이벤트 데이터 생성 스크립트
│
├── kafka/
│   ├── producer/                   # Kafka Producer 코드
│   ├── consumer/                   # Kafka Consumer 코드
│   └── config/                     # Kafka 관련 설정 파일
│
├── spark/
│   ├── batch/                      # Spark Batch 처리 작업
│   ├── streaming/                  # Spark Structured Streaming 작업
│   ├── jobs/                       # 공통 Spark Job 스크립트
│   └── config/                     # Spark 설정 파일
│
├── airflow/
│   ├── dags/                       # Airflow DAG 파일
│   ├── logs/                       # Airflow 실행 로그
│   └── plugins/                    # Airflow 커스텀 플러그인
│
├── db/
│   ├── init/                       # DB 초기화 SQL
│   └── models/                     # 테이블 설계 및 데이터 모델링 문서
│
├── notebooks/                      # 실험 및 데이터 탐색용 노트북
│
├── docs/
│   ├── architecture/               # 아키텍처 다이어그램
│   ├── event_schema/               # 이벤트 스키마 문서
│   └── data_modeling/              # Fact / Dimension 설계 문서
│
└── scripts/                        # 실행 보조 스크립트
```

---

## 3. 🔄 데이터 흐름 (Data Flow)

```text
[Batch Pipeline]

[Raw Data]
Olist CSV + Persona Data
    ↓
[Bronze Layer] (MinIO)
원본 데이터 Parquet 변환
    ↓
[Silver Layer] (MinIO)
정제 / 조인 / 타입 변환
    ↓
[Gold Layer]
- MinIO (Parquet) → 보관 및 재처리
- PostgreSQL → 조회 및 BI

[Streaming Pipeline]
Kafka (Event Data)
    ↓
Spark Structured Streaming
    ↓
Real-time Metrics (PostgreSQL)

```
### 📥 Input Data (유입 데이터)

#### 1. Olist E-Commerce Data (정적 데이터)

- orders
- order_items
- customers
- products
- sellers
- payments
- reviews

#### 2. Persona Data (보강 데이터) 

- name
- age
- gender

#### 3. Event Data (스트리밍 데이터)

- ORDER_CREATED
- ORDER_APPROVED
- ORDER_CANCELED
- ORDER_DELIVERED
- DELIVERY_STARTED
- DELIVERY_COMPLETED
- REVIEW_CREATED

Kafka Topics:

- order-events
- delivery-events
- review-events
---

## 4. 🧰 사용 기술 (Tech Stack)

### 📦 Data Processing
![Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-FF9900?style=for-the-badge&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

---

### 🚀 Data Streaming
![Kafka](https://img.shields.io/badge/Apache%20Kafka-000000?style=for-the-badge&logo=apachekafka&logoColor=white)
![Zookeeper](https://img.shields.io/badge/ZooKeeper-FF0000?style=for-the-badge&logo=apache&logoColor=white)

---

### 🐳 Infrastructure
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Docker Compose](https://img.shields.io/badge/Docker%20Compose-1488C6?style=for-the-badge&logo=docker&logoColor=white)
![Oracle Cloud](https://img.shields.io/badge/Oracle%20Cloud-F80000?style=for-the-badge&logo=oracle&logoColor=white)

---

### 📊 Data Format
![JSON](https://img.shields.io/badge/JSONL-000000?style=for-the-badge&logo=json&logoColor=white)
![CSV](https://img.shields.io/badge/CSV-217346?style=for-the-badge&logo=microsoftexcel&logoColor=white)

---

### 🛠️ Development / Utility
![Faker](https://img.shields.io/badge/Faker-3DDC84?style=for-the-badge&logo=python&logoColor=white)
![Kafka UI](https://img.shields.io/badge/Kafka%20UI-2C2C2C?style=for-the-badge&logo=apachekafka&logoColor=white)
