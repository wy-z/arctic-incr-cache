Feature: Cache miss
  When no data exists in ArcticDB, fetch from upstream and store.

  Scenario: First request fetches and stores data
    Given an empty ArcticDB library
    And an upstream source with 15 daily bars from "2024-01-01"
    When I request 10 bars for "S" ending "2024-01-15"
    Then the result has 10 rows
    And the data is stored in ArcticDB

  Scenario: Empty upstream returns empty result
    Given an empty ArcticDB library
    And an upstream source with no data
    When I request 10 bars for "S" ending "2024-01-15"
    Then the result is empty
    And nothing is stored in ArcticDB
