require 'rails_helper'

RSpec.describe FacebookAdsController, type: :controller do
  let(:valid_response) { '{"data": [{"id": "act_123"}]}' }
  let(:valid_insights_response) { '{"data": [{"account_name": "Test Account", "ctr": "0.5", "inline_link_click_ctr": "0.4", "clicks": "10", "inline_link_clicks": "5", "cost_per_inline_link_click": "0.2", "impressions": "1000", "spend": "50", "actions": [{"action_type": "mobile_app_install", "value": "2"}, {"action_type": "like", "value": "3"}, {"action_type": "landing_page_view", "value": "4"}, {"action_type": "video_view", "value": "5"}, {"action_type": "comment", "value": "1"}]}]}' }
  let(:db) { double('SQLite3::Database') }

  before do
    allow(controller).to receive(:open_db_connection).and_return(db)
    allow(controller).to receive(:close_db_connection)
    allow(db).to receive(:execute)
    allow(Concurrent::Promises).to receive(:future_on).and_yield
    allow_any_instance_of(Logger).to receive(:error)
  end

  describe '#fetch_and_store_account_insights' do
    it 'fetches account ids and stores account insights' do
      allow(controller).to receive(:fetch_accounts).and_return(['act_123'])
      allow(controller).to receive(:fetch_and_store_account_insights_data)
      allow(Date).to receive(:parse).with('2024-03-01').and_return(Date.new(2024, 3, 1))
      allow(Date).to receive(:parse).with('2024-05-14').and_return(Date.new(2024, 5, 14))

      expect(controller).to receive(:fetch_accounts)
      expect(controller).to receive(:fetch_and_store_account_insights_data).exactly((Date.new(2024, 5, 14) - Date.new(2024, 3, 1) + 1).to_i).times
      expect(db).to receive(:execute).with(any_args).at_least(:once)

      get :fetch_and_store_account_insights

      expect(response).to have_http_status(:success)
      expect(response.body).to eq('"Done"')
    end

    it 'renders internal server error on exception' do
      allow(controller).to receive(:fetch_accounts).and_raise(StandardError.new('Something went wrong'))

      get :fetch_and_store_account_insights

      expect(response).to have_http_status(:internal_server_error)
      expect(JSON.parse(response.body)['error']).to include('Internal Server Error')
    end
  end

  describe '#fetch_accounts' do
    it 'fetches ad account ids from Facebook API' do
      allow(controller).to receive(:`).with(any_args).and_return(valid_response)

      expect(controller.send(:fetch_accounts)).to eq(['act_123'])
    end

    it 'raises an error when unable to fetch accounts' do
      allow(controller).to receive(:`).with(any_args).and_return(nil)

      expect { controller.send(:fetch_accounts) }.to raise_error(ArgumentError, 'Unable to fetch accounts')
    end
  end

  describe '#fetch_and_store_account_insights_data' do
    it 'fetches and stores account insights data' do
      allow(controller).to receive(:`).with(any_args).and_return(valid_insights_response)
      allow(controller).to receive(:store_account_insights_data)

      expect(controller).to receive(:store_account_insights_data).with(hash_including(
        account_id: 'act_123',
        date: '2024-03-01',
        account_name: 'Test Account',
        ctr: 0.5,
        inline_link_click_ctr: 0.4,
        clicks: 10,
        inline_link_clicks: 5,
        cost_per_inline_link_click: 0.2,
        impressions: 1000,
        spend: 50,
        mobile_app_installs: 2,
        landing_page_view: 4,
        video_view: 5,
        likes: 3,
        comment: 1
      ), db)

      controller.send(:fetch_and_store_account_insights_data, 'act_123', Date.new(2024, 3, 1), db)
    end

    it 'does not store data when response is invalid' do
      allow(controller).to receive(:`).with(any_args).and_return(nil)

      expect(controller).not_to receive(:store_account_insights_data)

      controller.send(:fetch_and_store_account_insights_data, 'act_123', Date.new(2024, 3, 1), db)
    end
  end
end
