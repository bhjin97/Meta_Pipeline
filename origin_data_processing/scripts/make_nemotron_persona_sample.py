import os
import pandas as pd
from datasets import load_dataset

CUSTOMER_PATH = "data/raw/olist_customers_dataset.csv"
OUTPUT_PATH = "data/raw/nemotron_persona_korea_sample.csv"

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
    print("Columns:", persona_df.columns.tolist())

    text_col = "professional_persona" 

    persona_df["age_group"] = persona_df["age"].apply(get_age_group)

    target_counts = {
        age_group: int(customer_count * ratio)
        for age_group, ratio in AGE_RATIO.items()
    }

    diff = customer_count - sum(target_counts.values())
    target_counts["70+"] += diff

    sampled_list = []

    for age_group, count in target_counts.items():
        group_df = persona_df[persona_df["age_group"] == age_group]

        if len(group_df) < count:
            sampled = group_df.sample(n=count, replace=True, random_state=42)
        else:
            sampled = group_df.sample(n=count, replace=False, random_state=42)

        sampled_list.append(sampled)

    final_df = pd.concat(sampled_list, ignore_index=True)

    # 예: "전기태 씨는 ..." → "전기태"
    final_df["customer_name"] = final_df[text_col].str.split().str[0]

    final_df = final_df.sample(frac=1, random_state=42).reset_index(drop=True)

    final_df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

    print(f"Saved: {OUTPUT_PATH}")
    print(f"Final rows: {len(final_df)}")
    print(final_df["age_group"].value_counts(normalize=True).sort_index())
    if "gender" in final_df.columns:
        print(final_df["gender"].value_counts(normalize=True))


if __name__ == "__main__":
    main()