import os
import pandas as pd
from datasets import load_dataset

CUSTOMER_PATH = "data/raw/olist_customers_dataset.csv"
OUTPUT_PATH = "data/raw/nemotron_persona_korea_sample.parquet"

AGE_RATIO = {
    "20s": 0.05,
    "30s": 0.09,
    "40s": 0.11,
    "50s": 0.14,
    "60s": 0.15,
    "70+": 0.46,
}


def get_age_group(age):
    age = int(age)

    if age < 30:
        return "20s"
    elif age < 40:
        return "30s"
    elif age < 50:
        return "40s"
    elif age < 60:
        return "50s"
    elif age < 70:
        return "60s"
    else:
        return "70+"


def main():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    customers_df = pd.read_csv(CUSTOMER_PATH)
    customer_count = len(customers_df)

    print(f"Olist customer count: {customer_count}")

    dataset = load_dataset("nvidia/Nemotron-Personas-Korea")
    persona_df = dataset["train"].to_pandas()

    print(f"Nemotron rows: {len(persona_df)}")
    print("Original columns:", persona_df.columns.tolist())

    text_col = "professional_persona"

    # 컬럼명 통일: dim_customer 코드에서 sex를 사용하므로 gender -> sex
    if "gender" in persona_df.columns and "sex" not in persona_df.columns:
        persona_df = persona_df.rename(columns={"gender": "sex"})

    # 필수 컬럼 검증
    required_cols = [
        "age",
        "sex",
        "occupation",
        "marital_status",
        "education_level",
        "family_type",
        "housing_type",
        "province",
        "district",
        text_col,
    ]

    missing_cols = [c for c in required_cols if c not in persona_df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")

    persona_df["age"] = persona_df["age"].astype(int)
    persona_df["age_group"] = persona_df["age"].apply(get_age_group)

    # 예: "전기태 씨는 ..." → "전기태"
    persona_df["customer_name"] = (
        persona_df[text_col]
        .astype(str)
        .str.split()
        .str[0]
    )

    target_counts = {
        age_group: int(customer_count * ratio)
        for age_group, ratio in AGE_RATIO.items()
    }

    diff = customer_count - sum(target_counts.values())
    target_counts["70+"] += diff

    sampled_list = []

    for age_group, count in target_counts.items():
        group_df = persona_df[persona_df["age_group"] == age_group]

        if group_df.empty:
            raise ValueError(f"No persona rows for age_group: {age_group}")

        sampled = group_df.sample(
            n=count,
            replace=len(group_df) < count,
            random_state=42,
        )

        sampled_list.append(sampled)

    final_df = pd.concat(sampled_list, ignore_index=True)

    final_df = final_df.sample(frac=1, random_state=42).reset_index(drop=True)

    # Customer Dimension에서 사용할 컬럼만 명시적으로 저장
    output_cols = [
        "customer_name",
        "sex",
        "age",
        "age_group",
        "occupation",
        "marital_status",
        "education_level",
        "family_type",
        "housing_type",
        "province",
        "district",
        text_col,
    ]

    final_df = final_df[output_cols].rename(
        columns={
            text_col: "persona"
        }
    )

    # CSV 대신 Parquet 저장: 컬럼 밀림 방지
    final_df.to_parquet(OUTPUT_PATH, index=False)

    print(f"Saved: {OUTPUT_PATH}")
    print(f"Final rows: {len(final_df)}")
    print("\nAge group ratio:")
    print(final_df["age_group"].value_counts(normalize=True).sort_index())

    print("\nSex ratio:")
    print(final_df["sex"].value_counts(normalize=True))

    print("\nSample:")
    print(final_df.head(10))


if __name__ == "__main__":
    main()