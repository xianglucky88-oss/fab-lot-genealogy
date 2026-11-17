"""Stable output envelope."""
from .models import PipelineResult


def build_result(project: str, data: dict) -> dict:
    result = PipelineResult(project=project, data=dict(data))
    # Envelope fields are authoritative: data keys can never override them.
    return {**result.data, "project": result.project, "status": "ok"}
