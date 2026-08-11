Feature: Incomplete bars
  A bar that may still be updating is served but never stored — stored, it
  would sit there half-formed and shadow the finalised bar that follows.

  Scenario: Today is served but not stored
    Given an empty store
    And an upstream source with 15 daily bars ending today
    When I request 10 bars for "S" with no end date
    Then the result contains today
    And the store does not hold today
