"""Tests for catalog v1 parser, specifically testing extra fields handling."""
from vendor.dbt_artifacts_parser.parsers.catalog.catalog_v1 import Metadata


class TestMetadataExtraFields:
    """Test that Metadata class accepts extra fields from dbt."""

    def test_metadata_accepts_extra_fields(self):
        """Test that metadata accepts fields not explicitly defined in the model."""
        # Test with a new field that dbt might add in the future
        data = {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/catalog/v1.json",
            "dbt_version": "1.9.0",
            "generated_at": "2025-11-05T10:00:00Z",
            "invocation_id": "test-invocation-123",
            "invocation_started_at": "2025-11-05T09:59:00Z",  # New field
            "new_future_field": "some_value",  # Another potential future field
        }

        # This should not raise a validation error
        metadata = Metadata(**data)

        # Verify that known fields are accessible normally
        assert metadata.dbt_schema_version == "https://schemas.getdbt.com/dbt/catalog/v1.json"
        assert metadata.dbt_version == "1.9.0"
        assert metadata.generated_at == "2025-11-05T10:00:00Z"
        assert metadata.invocation_id == "test-invocation-123"

    def test_metadata_extra_fields_in_pydantic_extra(self):
        """Test that extra fields are stored in __pydantic_extra__."""
        data = {
            "dbt_version": "1.9.0",
            "invocation_started_at": "2025-11-05T09:59:00Z",
            "new_field_1": "value1",
            "new_field_2": 123,
        }

        metadata = Metadata(**data)

        # Extra fields should be stored in __pydantic_extra__
        assert metadata.__pydantic_extra__ is not None
        assert "invocation_started_at" in metadata.__pydantic_extra__
        assert "new_field_1" in metadata.__pydantic_extra__
        assert "new_field_2" in metadata.__pydantic_extra__
        assert metadata.__pydantic_extra__["invocation_started_at"] == "2025-11-05T09:59:00Z"
        assert metadata.__pydantic_extra__["new_field_1"] == "value1"
        assert metadata.__pydantic_extra__["new_field_2"] == 123

    def test_metadata_model_dump_includes_extra_fields(self):
        """Test that model_dump() includes extra fields."""
        data = {
            "dbt_version": "1.9.0",
            "invocation_id": "test-123",
            "invocation_started_at": "2025-11-05T09:59:00Z",
            "future_field": "future_value",
        }

        metadata = Metadata(**data)
        dumped = metadata.model_dump()

        # All fields including extra should be in the dump
        assert dumped["dbt_version"] == "1.9.0"
        assert dumped["invocation_id"] == "test-123"
        assert dumped["invocation_started_at"] == "2025-11-05T09:59:00Z"
        assert dumped["future_field"] == "future_value"

    def test_metadata_with_no_extra_fields(self):
        """Test that metadata works normally when no extra fields are provided."""
        data = {
            "dbt_version": "1.9.0",
            "generated_at": "2025-11-05T10:00:00Z",
        }

        metadata = Metadata(**data)

        assert metadata.dbt_version == "1.9.0"
        assert metadata.generated_at == "2025-11-05T10:00:00Z"

    def test_metadata_with_only_extra_fields(self):
        """Test that metadata accepts data with only extra fields (all known fields are Optional)."""
        data = {
            "some_new_field": "value",
            "another_new_field": 42,
        }

        # This should work since all defined fields are Optional
        metadata = Metadata(**data)

        assert metadata.__pydantic_extra__["some_new_field"] == "value"
        assert metadata.__pydantic_extra__["another_new_field"] == 42

    def test_invocation_started_at_as_extra_field(self):
        """Test the specific case of invocation_started_at being handled as an extra field."""
        # This is the real-world scenario: dbt adds invocation_started_at
        data = {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/catalog/v1.json",
            "dbt_version": "1.9.0",
            "generated_at": "2025-11-05T10:00:00Z",
            "invocation_id": "abc-123-def-456",
            "invocation_started_at": "2025-11-05T09:55:30.123456Z",
        }

        # Should not raise ValidationError
        metadata = Metadata(**data)

        # The field should be accessible via __pydantic_extra__
        assert metadata.__pydantic_extra__["invocation_started_at"] == "2025-11-05T09:55:30.123456Z"

        # And should be included in model_dump()
        dumped = metadata.model_dump()
        assert dumped["invocation_started_at"] == "2025-11-05T09:55:30.123456Z"
