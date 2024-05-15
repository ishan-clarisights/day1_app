require 'csv'

class RevenueDataMergeController < ApplicationController
  def read_csv
    file_path = Rails.root.join('abc.csv')
    
    revenues = process_csv(file_path)

    render json: JSON.pretty_generate(revenues)
  end

  private
    def process_csv(file_path)
      revenues = []

      CSV.foreach(file_path) do |row|
        revenues << {
          date: row[0],
          campaign_name: row[1],
          adset_name: row[2],
          ad_name: row[3],
          revenue: row[4].to_f
        }
      end

      revenues
    end

    def process_revenue_data(revenues)
      revenues.each do |revenue|
        next if revenue['date'].nil? || revenue['revenue'].nil?
        next if revenue['campaign_name'].nil? && revenue['adset_name'].nil? && revenue['ad_name'].nil?

        if (revenue['campaign_name'].nil? && revenue['adset_name'].nil?)
          # attribute the revenue to ad if able to
          next
        end

        if (revenue['campaign_name'].nil? && revenue['ad_name'].nil?)
          # attribute the revenue to adset if able to
          next
        end

        if (revenue['ad_name'].nil? && revenue['adset_name'].nil?)
          # attribute the revenue to campaign if able to
          next
        end

        if (revenue['campaign_name'].nil?)
          # find adsetid for adset and attribute if ad exists with the name
          next
        end

        if (revenue['adset_name'].nil?)
          # find all adsetids for campaign name. find all adids for those adset. attribute if ad exists with the name
          next
        end

        if (revenue['ad_name'].nil?)
          # find campaign for campaign and attribute if adset exists with the name
          next
        end

        


      end
    end
end
