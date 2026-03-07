Feature: Incomplete bar exclusion
  Bars that may still be updating are excluded from storage.

  Scenario: Today's bar is excluded from daily storage
    Given an empty ArcticDB library
    And an upstream source with 15 daily bars ending today
    When I request 10 bars for "S" with no end date
    Then the stored data does not include today
