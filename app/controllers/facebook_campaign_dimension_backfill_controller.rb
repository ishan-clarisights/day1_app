class FacebookCampaignDimensionBackfillController < ApplicationController
  ACCESS_TOKEN = 'EAAKDX99BZAz4BO6bXJm9vZCL4mwZBhFZACtutPb3tsHsCiF2gRv1N22TzUIAViNPzhA1buIN83DNSp6Q83s5CBRrnRKSqEWiTDBjDkrhU4kKhl3BIDZA2CV3wgB5xu2zxlUP5GwlUrB6oEJOZCrBGADWCZAZCw3X16zWVdZACsv8D3lKht8Pg3rb7xganYXyo5Ko8'
  API_VERSION = 'v19.0'

  def fetch_and_store_campaign_dimension_data
    ad_account_ids = fetch_accounts

    campaign_ids = []

    threads = ad_account_ids.map do |account_id|
      Concurrent::Future.execute { fetch_campaign_ids_for_account(account_id) }
    end

    threads.each { |t| campaign_ids.concat(t.value) }

    create_campaign_dimensions_table

    thread_pool = Concurrent::ThreadPoolExecutor.new(min_threads: 1, max_threads: 500)

    campaign_ids.each do |campaign_id|
      Concurrent::Promises.future_on(thread_pool) do
        fetch_campaign_dimensions(campaign_id).then do |data|
          store_dimensions_for_campaign(data)
        end
      end
    end

    thread_pool.shutdown
    thread_pool.wait_for_termination

    render json: JSON.pretty_generate(campaign_ids.length())
  end

  private
    def fetch_accounts
      url = "https://graph.facebook.com/#{API_VERSION}/me/adaccounts?access_token=#{ACCESS_TOKEN}"

      response = `curl "#{url}"`

      ad_account_ids = JSON.parse(response)['data'].map { |account| account['id'] }

      return ad_account_ids
    end

    def fetch_campaign_ids_for_account(account_id)
      url = "https://graph.facebook.com/#{API_VERSION}/#{account_id}/campaigns?limit=10000&access_token=#{ACCESS_TOKEN}"

      response = `curl "#{url}"`

      campaign_ids = JSON.parse(response)['data'].map { |campaign| campaign['id'] }

      if !campaign_ids.nil? && !campaign_ids.empty?
        campaign_ids
      else
        []
      end
    end

    def fetch_campaign_dimensions(campaign_id)
      url = "https://graph.facebook.com/#{API_VERSION}/#{campaign_id}/?fields=name,account_id,start_time,objective,daily_budget,lifetime_budget,buying_type&access_token=#{ACCESS_TOKEN}"

      response = `curl "#{url}"`

      campaign_dimensions = JSON.parse(response)
    
      actual_start_date = campaign_dimensions["start_time"].split("T")[0]

      campaign_dimensions["start_date"] = actual_start_date

      campaign_dimensions
    end

    def create_campaign_dimensions_table
      db = SQLite3::Database.open 'insights.db'

      db.execute "CREATE TABLE IF NOT EXISTS campaign_dimensions (
          campaign_id VARCHAR(255),
          campaign_name VARCHAR(255),
          start_date VARCHAR(255),
          account_id VARCHAR(255),
          objective VARCHAR(255),
          daily_budget FLOAT,
          lifetime_budget FLOAT,
          buying_type VARCHAR(255),
          PRIMARY KEY (campaign_id)
        )"

      db.close
    end

    def store_dimensions_for_campaign(data)
      db = SQLite3::Database.open 'insights.db'

      db.execute("REPLACE INTO campaign_dimensions (campaign_id, campaign_name, start_date, account_id, objective, daily_budget, lifetime_budget, buying_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
              [data["id"], data["name"], data["start_date"], "act_" + data["account_id"], data["objective"], data["daily_budget"].to_f, data["lifetime_budget"].to_f, data["buying_type"]])

      db.close
    end
end

