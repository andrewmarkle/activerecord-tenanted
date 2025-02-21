require "test_helper"

Capybara.server = :puma, { Silent: true } # suppress server boot announcement

class ApplicationSystemTestCase < ActionDispatch::SystemTestCase
  driven_by :selenium, using: :headless_chrome, screen_size: [ 1400, 1400 ]
end
