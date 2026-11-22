"""Shared investigation state flowing through the LangGraph supervisor."""
from __future__ import annotations

from datetime import datetime
from typing import Annotated, TypedDict


def merge_errors(left: list, right: list | None) -> list:
    """Reducer for the errors channel - agent failures accumulate."""
    if right is None:
        return left
    return (left or []) + right


class InvestigationState(TypedDict, total=False):
    # request scope
    run_id: str
    question: str
    window_start: datetime
    window_end: datetime
    baseline_start: datetime
    metric_version: int
    config: dict          # external-source pinning: window/grain/data_source
    grain: str            # "hour" (intraday demo) | "day" (multi-month sources)

    # agent outputs (each dict carries its own provenance: templates + windows)
    plan: dict
    funnel: dict
    segments: dict
    anomaly: dict
    experiment: dict
    real_experiments: list[dict]
    report: dict

    # orchestration bookkeeping
    retries: dict[str, int]
    errors: Annotated[list[dict], merge_errors]


STATE_SCHEMA = InvestigationState
