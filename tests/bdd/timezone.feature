Feature: Timezone handling
  Data is stored in the configured timezone; queries convert parameters accordingly.

  Scenario: Store localizes naive data to configured timezone
    Given an empty ArcticDB library
    And an upstream source with 60 minute bars from "2024-01-15 09:30"
    When I request 30 bars for "S" ending "2024-01-15 10:30" with timezone "America/New_York"
    Then the stored data has timezone "America/New_York"

  Scenario: Read returns tz-naive data in configured timezone
    Given an ArcticDB library with 60 tz-aware minute bars in "America/New_York" from "2024-01-15 09:30"
    And an upstream source with no data
    When I request 30 bars for "S" ending "2024-01-15 10:30" with timezone "America/New_York"
    Then the result index is tz-naive
    And the result timestamps are in New York wall-clock time
