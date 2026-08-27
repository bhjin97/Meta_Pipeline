import os

from datasets import load_dataset


OUTPUT_PATH = "data/raw/nemotron_persona_korea_pool.parquet"


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
    os.makedirs(
        os.path.dirname(OUTPUT_PATH),
        exist_ok=True,
    )

    dataset = load_dataset(
        "nvidia/Nemotron-Personas-Korea"
    )

    persona_df = dataset["train"].to_pandas()

    print(f"Nemotron rows: {len(persona_df)}")
    print(
        "Original columns:",
        persona_df.columns.tolist(),
    )

    text_col = "professional_persona"

    # dim_customer와 컬럼명 통일
    if (
        "gender" in persona_df.columns
        and "sex" not in persona_df.columns
    ):
        persona_df = persona_df.rename(
            columns={"gender": "sex"}
        )

    required_cols = [
        "uuid",
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

    missing_cols = [
        column
        for column in required_cols
        if column not in persona_df.columns
    ]

    if missing_cols:
        raise ValueError(
            f"Missing columns: {missing_cols}"
        )

    # uuid 검증
    if persona_df["uuid"].isna().any():
        raise ValueError(
            "Null uuid exists in persona data"
        )

    if persona_df["uuid"].duplicated().any():
        raise ValueError(
            "Duplicate uuid exists in persona data"
        )

    persona_df["age"] = (
        persona_df["age"]
        .astype(int)
    )

    persona_df["age_group"] = (
        persona_df["age"]
        .apply(get_age_group)
    )

    # 예: "전기태 씨는 ..." → "전기태"
    persona_df["customer_name"] = (
        persona_df[text_col]
        .astype(str)
        .str.split()
        .str[0]
    )

    output_cols = [
        "uuid",
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

    final_df = (
        persona_df[output_cols]
        .rename(
            columns={
                text_col: "persona"
            }
        )
        .reset_index(drop=True)
    )

    final_df.to_parquet(
        OUTPUT_PATH,
        index=False,
    )

    print(f"Saved: {OUTPUT_PATH}")
    print(f"Final rows: {len(final_df)}")

    print("\nAge group ratio:")
    print(
        final_df["age_group"]
        .value_counts(normalize=True)
        .sort_index()
    )

    print("\nSex ratio:")
    print(
        final_df["sex"]
        .value_counts(normalize=True)
    )

    print("\nSample:")
    print(final_df.head(10))


if __name__ == "__main__":
    main()