import pytest

from flowsense import ConfigurationError, MappedTaskAggregation
from flowsense.cli.policy import build_analysis_policy


def test_build_analysis_policy_maps_cli_names_to_domain_fields() -> None:
    policy = build_analysis_policy(
        minimum_history=10,
        baseline_window=20,
        medium_threshold=2.5,
        high_threshold=4.0,
        critical_threshold=6.0,
        minimum_relative_dispersion=0.02,
        minimum_absolute_dispersion=0.005,
        mapped_task_aggregation=MappedTaskAggregation.MEAN,
        change_point_detection=False,
        change_point_minimum_segment_size=4,
        change_point_score_threshold=4.5,
        trend_detection=False,
        trend_minimum_observations=8,
        trend_score_threshold=4.5,
        trend_minimum_directional_consistency=0.75,
    )

    assert policy.minimum_history == 10
    assert policy.baseline_window == 20
    assert policy.mapped_task_aggregation is MappedTaskAggregation.MEAN
    assert policy.minimum_relative_dispersion == 0.02
    assert policy.minimum_absolute_dispersion == 0.005
    assert policy.change_point_detection_enabled is False
    assert policy.change_point_minimum_segment_size == 4
    assert policy.change_point_score_threshold == 4.5
    assert policy.trend_detection_enabled is False
    assert policy.trend_minimum_observations == 8
    assert policy.trend_score_threshold == 4.5
    assert policy.trend_minimum_directional_consistency == 0.75


def test_build_analysis_policy_preserves_domain_validation() -> None:
    with pytest.raises(ConfigurationError, match="positive and increasing"):
        build_analysis_policy(
            minimum_history=5,
            baseline_window=None,
            medium_threshold=4.0,
            high_threshold=3.0,
            critical_threshold=6.0,
            minimum_relative_dispersion=0.01,
            minimum_absolute_dispersion=0.001,
            mapped_task_aggregation=MappedTaskAggregation.MAX,
            change_point_detection=True,
            change_point_minimum_segment_size=3,
            change_point_score_threshold=3.5,
            trend_detection=True,
            trend_minimum_observations=5,
            trend_score_threshold=3.5,
            trend_minimum_directional_consistency=0.6,
        )
