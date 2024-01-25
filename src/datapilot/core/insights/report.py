def generate_ci_cd_report(insights_data):
    """
    Generates a CI/CD friendly report from DBT model insights.

    :param insights_data: List of DBTInsightResult objects.
    """
    divider = "-" * 80
    for insight in insights_data:
        print(divider)
        print(f"Project: {insight.package_name}")
        print(f"Model ID: {insight.model_unique_id}")
        print(f"Name: {insight.metadata['model']}")
        print(f"Message: {insight.message}")
        print(f"Reason: {insight.reason_to_flag}")
        print(f"Recommendation: {insight.recommendation}")
        print(divider)
