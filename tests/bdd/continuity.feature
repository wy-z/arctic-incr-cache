Feature: Continuity
  Say what "complete" means and the cache holds one invariant at both ends of
  the store: a holey frame is never written, a holey stored window is
  re-fetched.  Either way the frame reaches the caller.

  Scenario: A hole in the store is re-fetched
    Given a store holding daily bars from "2024-01-01" to "2024-01-14" missing "2024-01-05".."2024-01-08"
    And a continuity hook that expects every calendar day
    And an upstream source with 14 daily bars from "2024-01-01"
    When I request 10 bars for "S" ending "2024-01-14"
    Then the result contains "2024-01-05"
    And the store holds "2024-01-05"

  Scenario: A holey frame is served but never stored
    Given an empty store
    And a continuity hook that expects every calendar day
    And an upstream source with daily bars from "2024-01-01" to "2024-01-14" missing "2024-01-05".."2024-01-08"
    When I request 10 bars for "S" ending "2024-01-14"
    Then the result has 10 rows
    And nothing was stored

  Scenario: A gap fetch that skips the stored tail is caught on the next read
    Given a store holding 10 daily bars from "2024-01-01"
    And a continuity hook that expects every calendar day
    And an upstream source with 4 daily bars from "2024-01-12" starting at value 200
    When I request 10 bars for "S" ending "2024-01-15"
    Then the result contains "2024-01-12"
    And the store holds "2024-01-12"
    And the store does not hold "2024-01-11"
    When I request 10 bars for "S" ending "2024-01-15"
    Then the upstream was called 2 times
    And the upstream was asked for 10 bars
