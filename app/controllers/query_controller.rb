class QueryController < ApplicationController

  def test
    sql = %{
      select
        o.id,
        o.ark,
        o.erc_what,
        o.version_number,
        o.inv_owner_id,
        (
          select
            count(f.id)
          from
            inv_files f
          where
            f.inv_object_id=o.id
        ),
        (
          select
            sum(f.billable_size)
          from
            inv_files f
          where
            f.inv_object_id=o.id
        )
      from
        inv_objects o
      order by o.id asc
      limit 10;
    }
    run_query(
      sql: sql,
      params: {},
      title: '10 Objects',
      headers: ['Object Id','Ark', 'Title', 'Version', 'Owner', 'File Count', 'Billable Size'],
      types: ['cell', 'cell', 'cell', 'cell', 'owner', 'data', 'data']
    )
  end

  def large_object
    sql = %{
      select
        o.id,
        o.ark,
        o.inv_owner_id,
        count(f.id) as files,
        sum(f.billable_size) as tot
      from
        inv_files f
      inner join inv_objects o
        on f.inv_object_id=o.id
      group by
        o.id,
        o.ark,
        o.inv_owner_id
      having
        sum(f.billable_size) > 1073741824
      order by sum(f.billable_size) desc;
    }
    run_query(
      sql: sql,
      params: {},
      title: 'Large Objects',
      headers: ['Object Id','Ark', 'Owner', 'File Count', 'Billable Size'],
      types: ['cell', 'cell', 'owner', 'data', 'data']
    )
  end

  def many_files
    sql = %{
      select
        o.id,
        o.ark,
        o.inv_owner_id,
        count(f.id) as files,
        sum(f.billable_size) as tot
      from
        inv_files f
      inner join inv_objects o
        on f.inv_object_id=o.id
      group by
        o.id,
        o.ark,
        o.inv_owner_id
      having
        count(f.id) > 1000
      order by count(f.id) desc;
    }
    run_query(
      sql: sql,
      params: {},
      title: 'Many Files',
      headers: ['Object Id', 'Ark', 'Owner', 'File Count', 'Billable Size'],
      types: ['cell', 'cell', 'owner', 'data', 'data']
    )
  end

private
  def format(types, c, col)
    return col if (c >= types.length)
    type = types[c]
    if (type == 'data')

    end
    return col
  end

  def run_query(
    sql: 'select 1',
    params: {},
    title: 'Title',
    headers: [],
    types: []
  )
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
    render template: 'query/test',
      status: 200,
      locals: {
        title: title,
        headers: headers,
        types: types,
        data: data
      }
  end
end
