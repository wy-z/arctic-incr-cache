Feature: Cache miss
  Nothing stored yet: fetch the whole window, keep it, serve it.

  Scenario: A first request fetches and stores
    Given an empty store
    And an upstream source with 15 daily bars from "2024-01-01"
    When I request 10 bars for "S" ending "2024-01-15"
    Then the result has 10 rows
    And the store holds "2024-01-15"

  Scenario: An empty source stores nothing
    Given an empty store
    And an upstream source with no data
    When I request 10 bars for "S" ending "2024-01-15"
    Then the result is empty
    And nothing was stored
