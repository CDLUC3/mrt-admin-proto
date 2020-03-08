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

  def large_object
    sql = %{
      select
        o.id,
        o.ark,
        format(sum(f.billable_size), 0) as tot
      from
        inv_files f
      inner join inv_objects o
        on f.inv_object_id=o.id
      group by
        o.id,
        o.ark
      having
        sum(f.billable_size) > 1073741824
      order by sum(f.billable_size) desc;
    }
    run_query(sql, {}, "Large Objects")
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
