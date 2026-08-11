Feature: Source depth
  A window the source cannot deepen is asked about once per floor TTL,
  not on every read.

  Scenario: A source at its depth is not asked again while the floor holds
    Given a store holding 5 daily bars from "2024-01-11"
    And an upstream source with 5 daily bars from "2024-01-11"
    When I request 10 bars for "S" ending "2024-01-15"
    And I request 10 bars for "S" ending "2024-01-15"
    Then the upstream was called once
