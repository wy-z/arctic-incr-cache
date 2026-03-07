Feature: Incremental update
  When cached data is stale, fetch only the gap and merge.

  Scenario: Merge new data with cached data
    Given an ArcticDB library with 10 daily bars from "2024-01-01"
    And an upstream source with 10 daily bars from "2024-01-11" starting at value 200
    When I request 15 bars for "S" ending "2024-01-20"
    Then the result has 15 rows
    And the data is stored in ArcticDB

  Scenario: Unchanged overlap row is deduplicated
    Given an ArcticDB library with 10 daily bars from "2024-01-01"
    And an upstream source returning the last cached row unchanged plus 5 new bars from "2024-01-11"
    When I request 15 bars for "S" ending "2024-01-20"
    Then the stored data does not contain "2024-01-10"

  Scenario: Changed overlap row is kept
    Given an ArcticDB library with 10 daily bars from "2024-01-01"
    And an upstream source returning the last cached row changed plus 5 new bars from "2024-01-11"
    When I request 15 bars for "S" ending "2024-01-20"
    Then the stored data contains "2024-01-10"

  Scenario: Empty upstream returns existing data
    Given an ArcticDB library with 10 daily bars from "2024-01-01"
    And an upstream source with no data
    When I request 10 bars for "S" ending "2024-01-20"
    Then the result has 10 rows
    And nothing is stored in ArcticDB
