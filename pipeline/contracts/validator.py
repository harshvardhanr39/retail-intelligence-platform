import pandera as pa
from pipeline.contracts.quality_logger import log_quality_failures

def validate_and_log(df, validate_fn, source_name: str, run_id: str = None):
    """
    Run a contract validation function.
    - If validation passes: return the validated df
    - If validation fails: log violations to DB and return original df
    - Never fails the pipeline
    """
    try:
        validated_df = validate_fn(df)
        print(f"[{source_name}] ✅ Contract validation passed")
        return validated_df

    except pa.errors.SchemaErrors as e:
        failures = e.failure_cases
        print(f"[{source_name}] ⚠️  Contract violations found: {len(failures)} issues")
        print(failures[["check", "column", "failure_case"]].head(10).to_string())

        # Log to DB without failing pipeline
        log_quality_failures(source_name, failures, run_id)

        return df  # return original df so pipeline continues

    except Exception as e:
        print(f"[{source_name}] WARNING: Validation error: {e}")
        return df
