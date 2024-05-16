# spec/controllers/insights_controller_spec.rb
require 'rails_helper'

RSpec.describe InsightsController, type: :controller do
  describe "GET #new" do
    it "assigns @group_by_dimensions and @metrics" do
      get :new
      expect(assigns(:group_by_dimensions)).to eq(InsightsController::CAMPAIGN_LEVEL_GROUPBY_DIMENSIONS.merge(InsightsController::ADSET_LEVEL_GROUPBY_DIMENSIONS).merge(InsightsController::AD_LEVEL_GROUPBY_DIMENSIONS))
      expect(assigns(:metrics)).to eq(InsightsController::METRICS)
    end

    it "fetches and assigns @accounts" do
      allow_any_instance_of(InsightsController).to receive(:fetch_accounts).and_return({"Test Account" => "12345"})
      get :new
      expect(assigns(:accounts)).to eq({"Test Account" => "12345"})
    end
  end

  describe "POST #fetch_data" do
    let(:params) {
      {
        start_date: "2024-01-01",
        end_date: "2024-01-31",
        account_id: "12345",
        group_by_dimensions: ["campaign_id"],
        metrics: ["clicks"]
      }
    }

    before do
      allow_any_instance_of(InsightsController).to receive(:form_sql_query).and_return("SELECT * FROM insights")
      allow_any_instance_of(InsightsController).to receive(:form_aggregate_query).and_return("SELECT SUM(clicks) FROM insights")
      allow_any_instance_of(InsightsController).to receive(:fetch_accounts).and_return({"Test Account" => "12345"})
      allow(SQLite3::Database).to receive(:open).and_return(double("SQLite3::Database", execute: [["Test Data"]], close: true))
    end

    it "assigns @column_headers, @results, @metrics_agg, @account_id_to_display, and @account_name_to_display" do
      post :fetch_data, params: params

      expect(assigns(:column_headers)).to eq(["Campaign Id", "Clicks"])
      expect(assigns(:results)).to eq([["Aggregation", "Test Data"]])
      expect(assigns(:metrics_agg)).to eq([["Aggregation", "Test Data"]])
      expect(assigns(:account_id_to_display)).to eq("12345")
      expect(assigns(:account_name_to_display)).to eq("Test Account")
    end

    it "handles empty group_by_dimensions and metrics" do
      post :fetch_data, params: params.except(:group_by_dimensions, :metrics)

      expect(assigns(:column_headers)).to be_empty
    end
  end
end
