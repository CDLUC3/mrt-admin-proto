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
      types: ['', '', '', '', 'owner', 'data', 'data']
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
      types: ['', '', 'owner', 'data', 'data']
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
      types: ['', '', 'owner', 'data', 'data']
    )
  end

  def nodes
    sql = %{
      select
        number,
        description,
        (
          select
            count(*)
          from
            inv_nodes_inv_objects inio
          where
            n.id=inio.inv_node_id
        ) as total,
        (
          select
            count(*)
          from
            inv_nodes_inv_objects inio
          where
            n.id=inio.inv_node_id
          and
            role = 'primary'
        ),
        (
          select
            count(*)
          from
            inv_nodes_inv_objects inio
          where
            n.id=inio.inv_node_id
          and
            role = 'secondary'
        )
      from
        inv_nodes n
      order by
        total desc;
    }
    run_query(
      sql: sql,
      params: {},
      title: 'Storage Nodes',
      headers: ['Node Number', 'Description', 'Total Obj', 'Primary', 'Secondary'],
      types: ['', '', 'data', 'data', 'data']
    )
  end

  def coll_nodes
    sql = %{
      select
        c.id,
        c.name,
        n.number,
        n.description,
        (
          select
            count(*)
          from
            inv_nodes_inv_objects inio
            inner join inv_collections_inv_objects co
              on co.inv_object_id = inio.inv_object_id
            where
              n.id=inio.inv_node_id
            and
              c.id=co.inv_collection_id
        ) as total,
        (
          select
            count(*)
          from
            inv_nodes_inv_objects inio
          inner join inv_collections_inv_objects co
            on co.inv_object_id = inio.inv_object_id
          where
            n.id=inio.inv_node_id
          and
            c.id=co.inv_collection_id
          and
            role = 'primary'
        ),
        (
          select
            count(*)
          from
            inv_nodes_inv_objects inio
          inner join inv_collections_inv_objects co
            on co.inv_object_id = inio.inv_object_id
          where
            n.id=inio.inv_node_id
          and
            c.id=co.inv_collection_id
          and
            role = 'secondary'
        )
      from
        inv_nodes n,
        inv_collections c
      where exists (
        select 1
        from
          inv_nodes_inv_objects inio
        inner join inv_collections_inv_objects co
          on co.inv_object_id = inio.inv_object_id
        where
          n.id=inio.inv_node_id
        and
          c.id=co.inv_collection_id
      )
      order by
        c.id, n.number
    }
    run_query(
      sql: sql,
      params: {},
      title: 'Storage Nodes',
      headers: ['Collection Id', 'Collection Name','Node Number', 'Description', 'Total Obj', 'Primary', 'Secondary'],
      types: ['', '', '', '', 'data', 'data', 'data']
    )
  end

  def coll_nodes_by_node
    sql = %{
      select
        n.number,
        n.description,
        c.id,
        c.name,
        (
          select
            count(*)
          from
            inv_nodes_inv_objects inio
            inner join inv_collections_inv_objects co
              on co.inv_object_id = inio.inv_object_id
            where
              n.id=inio.inv_node_id
            and
              c.id=co.inv_collection_id
        ) as total,
        (
          select
            count(*)
          from
            inv_nodes_inv_objects inio
          inner join inv_collections_inv_objects co
            on co.inv_object_id = inio.inv_object_id
          where
            n.id=inio.inv_node_id
          and
            c.id=co.inv_collection_id
          and
            role = 'primary'
        ),
        (
          select
            count(*)
          from
            inv_nodes_inv_objects inio
          inner join inv_collections_inv_objects co
            on co.inv_object_id = inio.inv_object_id
          where
            n.id=inio.inv_node_id
          and
            c.id=co.inv_collection_id
          and
            role = 'secondary'
        )
      from
        inv_nodes n,
        inv_collections c
      where exists (
        select 1
        from
          inv_nodes_inv_objects inio
        inner join inv_collections_inv_objects co
          on co.inv_object_id = inio.inv_object_id
        where
          n.id=inio.inv_node_id
        and
          c.id=co.inv_collection_id
      )
      order by
        n.number, c.id
    }
    run_query(
      sql: sql,
      params: {},
      title: 'Storage Nodes',
      headers: ['Node Number', 'Description', 'Collection Id', 'Collection Name', 'Total Obj', 'Primary', 'Secondary'],
      types: ['', '', '', '', 'data', 'data', 'data']
    )
  end

  def coll_mime_types
    sql = %{
      select
        c.id,
        c.name,
        f.mime_type,
        count(*),
        sum(f.billable_size)
      from
        inv_collections c
      inner join inv_collections_inv_objects co
        on c.id = co.inv_collection_id
      inner join inv_files f
        on f.inv_object_id = co.inv_object_id
      where
        f.source = 'producer'
      group by c.id, c.name, f.mime_type
      order by c.id, count(*) desc;
    }
    run_query(
      sql: sql,
      params: {},
      title: 'Storage Nodes',
      headers: ['Collection Id', 'Collection Name', 'Mime Type', 'File Count', 'Billable Size'],
      types: ['', '', '', 'data', 'data']
    )
  end

  def mime_types
    sql = %{
      select
        f.mime_type,
        count(*),
        sum(f.billable_size)
      from
        inv_files f
      where
        f.source = 'producer'
      group by
        f.mime_type
      order by
        count(*) desc;
    }
    run_query(
      sql: sql,
      params: {},
      title: 'Storage Nodes',
      headers: ['Mime Type', 'File Count', 'Billable Size'],
      types: ['', 'data', 'data']
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
