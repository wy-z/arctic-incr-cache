Feature: Cache hit
  When fresh data exists in ArcticDB, serve directly without fetching.

  Scenario: Fresh cache skips upstream fetch
    Given an ArcticDB library with 20 daily bars from "2024-01-01"
    And an upstream source with no data
    When I request 10 bars for "S" ending "2024-01-15"
    Then the result has 10 rows
    And the upstream was not called
