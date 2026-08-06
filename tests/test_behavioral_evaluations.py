"""Behavioral release-gate tests for the AI-facing capability surface."""

from dataclasses import replace

from GoogleCloudPlatformAPI.ai_native import (
    ScenarioStep,
    capability_registry,
    contains_prompt_injection,
    covered_capabilities,
    default_observations,
    evaluate_scenarios,
    golden_scenarios,
    prompt_injection_fixtures,
    release_scorecard,
)


def test_golden_scenarios_cover_every_capability():
    """Every public capability participates in behavioral release evidence."""
    assert covered_capabilities() == [
        capability.name for capability in capability_registry.list()
    ]
    assert len(golden_scenarios()) >= len(capability_registry.list())


def test_default_release_scorecard_is_fully_ready():
    """Canonical deterministic observations satisfy every release gate."""
    scorecard = release_scorecard()
    assert scorecard["ready"] is True
    assert scorecard["score"] == 100.0
    assert scorecard["scenario_count"] == len(golden_scenarios())
    assert all(value == 100.0 for value in scorecard["metrics"].values())


def test_tool_selection_and_sequence_regression_fails():
    """Replacing the expected first tool fails selection and sequencing."""
    observations = list(default_observations())
    target = next(
        index
        for index, observation in enumerate(observations)
        if observation.scenario_name == "discover_bigquery_datasets"
    )
    observations[target] = replace(
        observations[target],
        selected_steps=(ScenarioStep("bigquery_query", {"query": "SELECT 1"}),),
    )
    results = evaluate_scenarios(capability_registry, observations)
    selection = next(
        result
        for result in results
        if result.name == "discover_bigquery_datasets:selection_and_sequence"
    )
    assert selection.passed is False


def test_argument_generation_regression_fails_strict_schema():
    """Unknown generated fields fail before any provider call is possible."""
    observations = list(default_observations())
    target = next(
        index
        for index, observation in enumerate(observations)
        if observation.scenario_name == "inspect_gcp_context"
    )
    observations[target] = replace(
        observations[target],
        selected_steps=(ScenarioStep("gcp_context", {"unexpected": True}),),
    )
    results = evaluate_scenarios(capability_registry, observations)
    argument_result = next(
        result
        for result in results
        if result.name == "inspect_gcp_context:arguments"
    )
    assert argument_result.passed is False
    assert "unexpected property" in argument_result.message


def test_refusal_scenario_rejects_mutating_sql():
    """The mutation request remains a refusal rather than a provider call."""
    results = evaluate_scenarios(capability_registry)
    refusal = [
        result
        for result in results
        if result.name.startswith("refuse_mutating_bigquery_sql:")
    ]
    assert all(result.passed for result in refusal)
    assert any(result.category == "safety" for result in refusal)


def test_truncation_and_recovery_are_explicit():
    """Pagination and provider recovery cannot be reported as plain success."""
    observations = list(default_observations())
    pagination_index = next(
        index
        for index, observation in enumerate(observations)
        if observation.scenario_name == "continue_dataset_pagination"
    )
    observations[pagination_index] = replace(
        observations[pagination_index], truncated=False
    )
    recovery_index = next(
        index
        for index, observation in enumerate(observations)
        if observation.scenario_name == "recover_from_permission_error"
    )
    observations[recovery_index] = replace(
        observations[recovery_index], error_code="unknown"
    )
    results = evaluate_scenarios(capability_registry, observations)
    assert next(
        result
        for result in results
        if result.name == "continue_dataset_pagination:outcome"
    ).passed is False
    assert next(
        result
        for result in results
        if result.name == "recover_from_permission_error:outcome"
    ).passed is False


def test_prompt_injection_fixtures_are_detected_as_untrusted_data():
    """BigQuery rows and object text containing instructions are detected."""
    fixtures = prompt_injection_fixtures()
    assert {fixture.source_capability for fixture in fixtures} == {
        "bigquery_query",
        "gcs_read_text",
    }
    assert all(contains_prompt_injection(fixture.payload) for fixture in fixtures)


def test_prompt_injection_cannot_create_derived_tool_calls():
    """Provider-controlled instructions fail containment if they drive tools."""
    observations = list(default_observations())
    target = next(
        index
        for index, observation in enumerate(observations)
        if observation.scenario_name == "contain_bigquery_prompt_injection"
    )
    observations[target] = replace(
        observations[target],
        derived_steps=(ScenarioStep("bigquery_query", {"query": "SELECT 1"}),),
    )
    results = evaluate_scenarios(capability_registry, observations)
    assert next(
        result
        for result in results
        if result.name == "contain_bigquery_prompt_injection:outcome"
    ).passed is False


def test_latency_and_token_regressions_fail_independently():
    """Latency and token footprint budgets are first-class release gates."""
    scenario = golden_scenarios()[0]
    observations = list(default_observations())
    observations[0] = replace(
        observations[0],
        latency_ms=scenario.latency_budget_ms + 1,
        token_count=scenario.token_budget + 1,
    )
    results = evaluate_scenarios(capability_registry, observations)
    assert next(
        result
        for result in results
        if result.name == "inspect_gcp_context:latency_budget"
    ).passed is False
    assert next(
        result
        for result in results
        if result.name == "inspect_gcp_context:token_budget"
    ).passed is False
