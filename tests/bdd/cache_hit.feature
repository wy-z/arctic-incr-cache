Feature: Cache hit
  A stored window that already answers the ask is served untouched.

  Scenario: A fresh window skips the source
    Given a store holding 20 daily bars from "2024-01-01"
    And an upstream source with no data
    When I request 10 bars for "S" ending "2024-01-15"
    Then the result has 10 rows
    And the upstream was not called
