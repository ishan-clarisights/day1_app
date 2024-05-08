require "test_helper"

class InsightsControllerTest < ActionDispatch::IntegrationTest
  test "should get new" do
    get insights_new_url
    assert_response :success
  end
end
