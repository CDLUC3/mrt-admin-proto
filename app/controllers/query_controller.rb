class QueryController < ApplicationController

  def test
    sql = %{
      SELECT
        count(*)
      FROM
        inv_objects
    }
    run_query(sql, {}, "Test Query")
  end

  private

  def run_query(sql, params, title)
    results = ActiveRecord::Base
      .connection
      .raw_connection
      .prepare(sql)
      .execute(params)

    data = []
    if results.present?
      results.each do |row|
        data.push(row)
      end
    end
    render status: 200, locals: {title: title, data: data}
  end
end
