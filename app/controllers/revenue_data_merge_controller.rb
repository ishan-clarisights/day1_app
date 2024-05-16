require 'csv'

class RevenueDataMergeController < ApplicationController
  def read_csv
    file_path = Rails.root.join('abc.csv')
    
    revenues = process_csv(file_path)

    db = open_db_connection

    process_revenue_data(revenues, db)

    close_db_connection(db)

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

    def process_revenue_data(revenues, db)
      revenues.each do |revenue|
        next if revenue[:date].nil? || revenue[:campaign_name].nil? || revenue[:revenue].nil? || revenue[:revenue] == 0.0
        
        campaign_id_result = find_campaign_id_for_name(db, revenue[:campaign_name])

        next if campaign_id_result.nil? || campaign_id_result.empty? || campaign_id_result[0].nil? || campaign_id_result[0].empty? || campaign_id_result[0][0].nil?
          
        campaign_id = campaign_id_result[0][0]

        if revenue[:adset_name].nil? || revenue[:adset_name].empty?
          associate_revenue_with_campaign(db, campaign_id, revenue[:revenue], revenue[:date])
          next
        end
        
        adset_id_result = find_adset_id_for_campaign_and_name(db, campaign_id, revenue[:adset_name])

        if adset_id_result.nil? || adset_id_result.empty? || adset_id_result[0].nil? || adset_id_result[0].empty? || adset_id_result[0][0].nil?
          associate_revenue_with_campaign(db, campaign_id, revenue[:revenue], revenue[:date])
          next
        end

        adset_id = adset_id_result[0][0]

        if revenue[:ad_name].nil? || revenue[:ad_name].empty?
          associate_revenue_with_campaign(db, campaign_id, revenue[:revenue], revenue[:date])
          associate_revenue_with_adset(db, adset_id, revenue[:revenue], revenue[:date])
          next
        end

        ad_id_result = find_ad_id_for_adset_and_name(db, adset_id, revenue[:ad_name])

        if ad_id_result.nil? || ad_id_result.empty? || ad_id_result[0].nil? || ad_id_result[0].empty? || ad_id_result[0][0].nil?
          associate_revenue_with_campaign(db, campaign_id, revenue[:revenue], revenue[:date])
          associate_revenue_with_adset(db, adset_id, revenue[:revenue], revenue[:date])
          next
        end

        ad_id = ad_id_result[0][0]

        associate_revenue_with_campaign(db, campaign_id, revenue[:revenue], revenue[:date])
        associate_revenue_with_adset(db, adset_id, revenue[:revenue], revenue[:date])
        associate_revenue_with_ad(db, ad_id, revenue[:revenue], revenue[:date])
      end
    end

    def open_db_connection
      db = SQLite3::Database.open 'insights.db'
      db
    end

    def close_db_connection(db)
      db.close
    end

    def find_campaign_id_for_name(db, campaign_name)
      db.execute("SELECT campaign_id from campaign_dimensions where campaign_name = ?", campaign_name);
    end

    def find_adset_id_for_campaign_and_name(db, campaign_id, adset_name)
      db.execute("SELECT adset_id from adset_dimensions where campaign_id = ? and adset_name = ?", campaign_id, adset_name);
    end

    def find_ad_id_for_adset_and_name(db, adset_id, ad_name)
      db.execute("SELECT ad_id from ad_dimensions where adset_id = ? and ad_name = ?", adset_id, ad_name);
    end

    def associate_revenue_with_campaign(db, campaign_id, revenue, date)
      result = db.execute("SELECT revenue from campaign_insights where campaign_id = ? and date = ?", campaign_id, date);

      if !result.nil? && !result.empty? && !result[0].nil? && !result[0].empty? && !result[0][0].nil?
        db.execute("UPDATE campaign_insights SET revenue = ? where campaign_id = ? and date = ?", result[0][0] + revenue, campaign_id, date);
      elsif
        result1 = db.execute("SELECT account_id from campaign_dimensions where campaign_id = ?", campaign_id);
  
        db.execute("INSERT INTO campaign_insights (campaign_id, date, account_id, revenue) VALUES (?, ?, ?, ?)",
                  [campaign_id, date, result1[0][0], revenue])
      end
    end

    def associate_revenue_with_adset(db, adset_id, revenue, date)
      result = db.execute("SELECT revenue from adset_insights where adset_id = ? and date = ?", adset_id, date);

      if !result.nil? && !result.empty? && !result[0].nil? && !result[0].empty? && !result[0][0].nil?
        db.execute("UPDATE adset_insights SET revenue = ? where adset_id = ? and date = ?", result[0][0] + revenue, adset_id, date);
      elsif
        result1 = db.execute("SELECT campaign_id, account_id from adset_insights where adset_id = ? limit 1", adset_id);
          
        if !result1.nil? && !result1.empty? && !result1[0].nil? && !result1[0].empty?
          db.execute("INSERT INTO adset_insights (adset_id, date, campaign_id, account_id, revenue) VALUES (?, ?, ?, ?, ?)",
                    [adset_id, date, result1[0][0], result1[0][1], revenue])
        end
      end
    end

    def associate_revenue_with_ad(db, ad_id, revenue, date)
      result = db.execute("SELECT revenue from ad_insights where ad_id = ? and date = ?", ad_id, date);

      if !result.nil? && !result.empty? && !result[0].nil? && !result[0].empty? && !result[0][0].nil?
        db.execute("UPDATE ad_insights SET revenue = ? where ad_id = ? and date = ?", result[0][0] + revenue, ad_id, date);
      elsif
        result = db.execute("SELECT adset_id, account_id from ad_insights where ad_id = ? limit 1", ad_id);

        if !result.nil? && !result.empty? && !result[0].nil? && !result[0].empty?
          db.execute("INSERT INTO ad_insights (ad_id, date, adset_id, account_id, revenue) VALUES (?, ?, ?, ?, ?)",
                  [ ad_id, date, result[0][0], result[0][1], revenue])
        end
      end
    end
end
