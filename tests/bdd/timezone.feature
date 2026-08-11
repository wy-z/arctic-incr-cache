Feature: Timezone
  Data is stored and returned in the symbol's configured market timezone,
  whatever timezone the source reports in.

  Scenario: A fetched frame is converted to the market timezone
    Given a market timezone of "America/New_York"
    And minute bars
    And an empty store
    And an upstream source reporting 60 minute bars in UTC from "2024-01-15 09:30"
    When I request 30 bars for "S" ending "2024-01-15 10:30"
    Then the written frame is in "America/New_York"

  Scenario: A stored frame is returned in the market timezone
    Given a market timezone of "America/New_York"
    And minute bars
    And a store holding 60 minute bars in "America/New_York" from "2024-01-15 09:30"
    And an upstream source with no data
    When I request 30 bars for "S" ending "2024-01-15 10:30"
    Then the result is in "America/New_York"
