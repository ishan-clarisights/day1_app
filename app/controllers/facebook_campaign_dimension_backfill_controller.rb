require Rails.root.join('config/initializers/constants')

class FacebookCampaignDimensionBackfillController < ApplicationController
  def initialize
    @logger = Logger.new(STDOUT)
    @logger.formatter = proc do |severity, datetime, progname, msg|
      "#{severity}: #{msg}\n"
    end
  end

  def fetch_and_store_campaign_dimension_data
    begin
      ad_account_ids = fetch_accounts

      campaign_ids = []

      threads = ad_account_ids.map do |account_id|
        Concurrent::Future.execute { fetch_campaign_ids_for_account(account_id) }
      end

      threads.each { |t| 
        if (!t.nil? && !t.value.nil?) 
          campaign_ids.concat(t.value)
        end
      }

      @logger.info("Total campaigns to get dimensions for: #{campaign_ids.length()}")

      db = open_db_connection

      create_campaign_dimensions_table(db)

      thread_pool = Concurrent::ThreadPoolExecutor.new(min_threads: 1, max_threads: 1)

      campaign_ids.each do |campaign_id|
        Concurrent::Promises.future_on(thread_pool) do
          fetch_campaign_dimensions(campaign_id).then do |data|
            if !data.nil?
              store_dimensions_for_campaign(data, db)
            end
          end
        end
      end

      thread_pool.shutdown
      thread_pool.wait_for_termination
      
      close_db_connection(db)

      render json: JSON.pretty_generate("Fetched dimensions for #{campaign_ids.length()} campaigns.")
    rescue StandardError => e
      render json: { error: "Internal Server Error. #{e.class}: #{e.message}" }, status: :internal_server_error
    end
  end

  private
    def fetch_accounts
      url = "https://graph.facebook.com/#{API_VERSION}/me/adaccounts?access_token=#{ACCESS_TOKEN}"

      response = `curl "#{url}"`

      if(response.nil? || JSON.parse(response).nil? || JSON.parse(response)['data'].nil? )
        raise ArgumentError, 'Unable to fetch accounts'
      end

      ad_account_ids = JSON.parse(response)['data'].map { |account| account['id'] }

      return ad_account_ids
    end

    def fetch_campaign_ids_for_account(account_id)
      url = "https://graph.facebook.com/#{API_VERSION}/#{account_id}/campaigns?limit=10000&access_token=#{ACCESS_TOKEN}"

      response = `curl "#{url}"`

      if(response.nil? || JSON.parse(response).nil? ||  JSON.parse(response)['data'].nil?)
        raise ArgumentError, "Did not receive a valid response while fetching campaigns for account: #{account_id}"
      end

      campaign_ids = JSON.parse(response)['data'].map { |campaign| campaign['id'] }

      if !campaign_ids.nil? && !campaign_ids.empty?
        campaign_ids
      else
        @logger.warn("No campaigns found for account: #{account_id}")
        []
      end
    end

    def fetch_campaign_dimensions(campaign_id)
      url = "https://graph.facebook.com/#{API_VERSION}/#{campaign_id}/?fields=name,account_id,start_time,objective,daily_budget,lifetime_budget,buying_type&access_token=#{ACCESS_TOKEN}"

      response = `curl "#{url}"`

      campaign_dimensions = JSON.parse(response)

      if !campaign_dimensions.nil? && !campaign_dimensions["start_time"].nil? && !campaign_dimensions["start_time"].split("T").nil? && !campaign_dimensions["start_time"].split("T")[0].nil?
        actual_start_date = campaign_dimensions["start_time"].split("T")[0]
        campaign_dimensions["start_date"] = actual_start_date
      end

      campaign_dimensions
    end

    def open_db_connection
      db = SQLite3::Database.open 'insights.db'
      db
    end

    def close_db_connection(db)
      db.close
    end

    def create_campaign_dimensions_table(db)
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
    end

    def store_dimensions_for_campaign(data)
      db.execute("REPLACE INTO campaign_dimensions (campaign_id, campaign_name, start_date, account_id, objective, daily_budget, lifetime_budget, buying_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
              [data["id"], data["name"], data["start_date"], "act_" + data["account_id"], data["objective"], data["daily_budget"].to_f, data["lifetime_budget"].to_f, data["buying_type"]])
    end
end

