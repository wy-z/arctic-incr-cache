Feature: Incremental update
  A stale tail costs one gap-sized fetch, not the whole window.  The ask
  overlaps the stored tail by one bar so a revision can land — unless the gap
  is wider than the window, which caps the ask at the window and leaves no
  room for the overlap.

  Scenario: A gap wider than the window caps the fetch at the window
    Given a store holding 10 daily bars from "2024-01-01"
    And an upstream source with 10 daily bars from "2024-01-11" starting at value 200
    When I request 10 bars for "S" ending "2024-01-20"
    Then the result has 10 rows
    And the upstream was asked for 10 bars
    And the store holds "2024-01-20"

  Scenario: An unchanged overlap bar is not rewritten
    Given a store holding 10 daily bars from "2024-01-01"
    And an upstream source returning the stored tail unchanged plus 5 new bars from "2024-01-11"
    When I request 10 bars for "S" ending "2024-01-20"
    Then the written frame does not contain "2024-01-10"

  Scenario: A revised overlap bar is rewritten
    Given a store holding 10 daily bars from "2024-01-01"
    And an upstream source returning the stored tail changed plus 5 new bars from "2024-01-11"
    When I request 10 bars for "S" ending "2024-01-20"
    Then the written frame contains "2024-01-10"

  Scenario: An empty source leaves the cache as it is
    Given a store holding 10 daily bars from "2024-01-01"
    And an upstream source with no data
    When I request 10 bars for "S" ending "2024-01-20"
    Then the result has 10 rows
    And nothing was stored
