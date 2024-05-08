class FacebookAdSetDimensionBackfillController < ApplicationController
  ACCESS_TOKEN = 'EAAKDX99BZAz4BO6bXJm9vZCL4mwZBhFZACtutPb3tsHsCiF2gRv1N22TzUIAViNPzhA1buIN83DNSp6Q83s5CBRrnRKSqEWiTDBjDkrhU4kKhl3BIDZA2CV3wgB5xu2zxlUP5GwlUrB6oEJOZCrBGADWCZAZCw3X16zWVdZACsv8D3lKht8Pg3rb7xganYXyo5Ko8'
  API_VERSION = 'v19.0'

  def fetch_and_store_adset_dimension_data
    ad_account_ids = fetch_accounts

    adset_ids = []

    threads = ad_account_ids.map do |account_id|
      Concurrent::Future.execute { fetch_adset_ids_for_account(account_id) }
    end

    threads.each { |t| adset_ids.concat(t.value) }

    create_adset_dimensions_table

    thread_pool = Concurrent::ThreadPoolExecutor.new(min_threads: 1, max_threads: 500)

    adset_ids.each do |adset_id|
      Concurrent::Promises.future_on(thread_pool) do
        fetch_adset_dimensions(adset_id).then do |data|
          store_dimensions_for_adset(data)
        end
      end
    end

    thread_pool.shutdown
    thread_pool.wait_for_termination

    render json: JSON.pretty_generate(adset_ids.length())
  end

  private
    def fetch_accounts
      url = "https://graph.facebook.com/#{API_VERSION}/me/adaccounts?access_token=#{ACCESS_TOKEN}"

      response = `curl "#{url}"`

      ad_account_ids = JSON.parse(response)['data'].map { |account| account['id'] }

      return ad_account_ids
    end

    def fetch_adset_ids_for_account(account_id)
      url = "https://graph.facebook.com/#{API_VERSION}/#{account_id}/adsets?limit=10000&access_token=#{ACCESS_TOKEN}"

      response = `curl "#{url}"`

      adset_ids = JSON.parse(response)['data'].map { |adset| adset['id'] }

      if !adset_ids.nil? && !adset_ids.empty?
        adset_ids
      else
        []
      end
    end

    def fetch_adset_dimensions(adset_id)
      url = "https://graph.facebook.com/#{API_VERSION}/#{adset_id}/?fields=name,campaign_id,start_time,optimization_goal,daily_budget,lifetime_budget,billing_event,bid_strategy&access_token=#{ACCESS_TOKEN}"

      response = `curl "#{url}"`

      adset_dimensions = JSON.parse(response)
    
      actual_start_date = adset_dimensions["start_time"].split("T")[0]

      adset_dimensions["start_date"] = actual_start_date

      adset_dimensions
    end

    def create_adset_dimensions_table
      db = SQLite3::Database.open 'insights.db'

      db.execute "CREATE TABLE IF NOT EXISTS adset_dimensions (
          adset_id VARCHAR(255),
          adset_name VARCHAR(255),
          start_date VARCHAR(255),
          campaign_id VARCHAR(255),
          optimization_goal VARCHAR(255),
          daily_budget FLOAT,
          lifetime_budget FLOAT,
          billing_event VARCHAR(255),
          bid_strategy VARCHAR(255),
          PRIMARY KEY (adset_id)
        )"

      db.close
    end

    def store_dimensions_for_adset(data)
      db = SQLite3::Database.open 'insights.db'

      db.execute("REPLACE INTO adset_dimensions (adset_id, adset_name, start_date, campaign_id, optimization_goal, daily_budget, lifetime_budget, billing_event, bid_strategy) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
              [data["id"], data["name"], data["start_date"], data["campaign_id"], data["optimization_goal"], data["daily_budget"].to_f, data["lifetime_budget"].to_f, data["billing_event"], data["bid_strategy"]])

      db.close
    end
end
