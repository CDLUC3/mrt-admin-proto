class QueryController < ApplicationController

  def objects
    ark = "#{params['ark'].strip}%"
    sql = %{
      select
        o.id,
        o.ark,
        o.erc_what,
        o.version_number,
        c.id,
        c.mnemonic,
        (
          select
            count(f.id)
          from
            inv.inv_files f
          where
            f.inv_object_id=o.id
        ),
        (
          select
            sum(f.billable_size)
          from
            inv.inv_files f
          where
            f.inv_object_id=o.id
        )
      from
        inv.inv_objects o
      inner join inv.inv_collections_inv_objects icio
        on o.id = icio.inv_object_id
      inner join inv.inv_collections c
        on icio.inv_collection_id = c.id
      where o.ark like ?
      order by o.id asc
      limit 20;
    }
    run_query(
      sql: sql,
      params: [ark],
      title: "Object(s) by Ark: #{ark}",
      headers: ['Object Id','Ark', 'Title', 'Version', 'Coll Id', 'Collection', 'File Count', 'Billable Size'],
      types: ['', 'ark', '', '', 'coll', 'mnemonic', 'dataint', 'dataint']
    )
  end

  def objects_by_title
    title = "%#{params['title'].strip}%"
    sql = %{
      select
        o.id,
        o.ark,
        o.erc_what,
        o.version_number,
        c.id,
        c.mnemonic,
        (
          select
            count(f.id)
          from
            inv.inv_files f
          where
            f.inv_object_id=o.id
        ),
        (
          select
            sum(f.billable_size)
          from
            inv.inv_files f
          where
            f.inv_object_id=o.id
        )
      from
        inv.inv_objects o
      inner join inv.inv_collections_inv_objects icio
        on icio.inv_object_id = o.id
      inner join inv.inv_collections c
        on icio.inv_collection_id = c.id
      where o.erc_what like ?
      order by o.id asc
      limit 20;
    }
    run_query(
      sql: sql,
      params: [title],
      title: "Object(s) by Title: #{title}",
      headers: ['Object Id','Ark', 'Title', 'Version', 'Coll Id', 'Collection', 'File Count', 'Billable Size'],
      types: ['', 'ark', '', '', 'coll', 'mnemonic', 'dataint', 'dataint']
    )
  end

  def objects_by_author
    author = "%#{params['author'].strip}%"
    sql = %{
      select
        o.id,
        o.ark,
        o.erc_what,
        o.version_number,
        c.id,
        c.mnemonic,
        (
          select
            count(f.id)
          from
            inv.inv_files f
          where
            f.inv_object_id=o.id
        ),
        (
          select
            sum(f.billable_size)
          from
            inv.inv_files f
          where
            f.inv_object_id=o.id
        )
      from
        inv.inv_objects o
      inner join inv.inv_collections_inv_objects icio
        on icio.inv_object_id = o.id
      inner join inv.inv_collections c
        on icio.inv_collection_id = c.id
      where o.erc_who like ?
      order by o.id asc
      limit 20;
    }
    run_query(
      sql: sql,
      params: [author],
      title: "Object(s) by Author: #{author}",
      headers: ['Object Id','Ark', 'Title', 'Version', 'Coll Id', 'Collection', 'File Count', 'Billable Size'],
      types: ['', 'ark', '', '', 'coll', 'mnemonic', 'dataint', 'dataint']
    )
  end

  def objects_by_file_coll
    file = "producer/#{params['file'].strip}"
    coll = params['coll'].strip
    sql = %{
      select
        o.id,
        o.ark,
        o.erc_what,
        o.version_number,
        c.id,
        c.mnemonic,
        (
          select
            count(f.id)
          from
            inv.inv_files f
          where
            f.inv_object_id=o.id
        ),
        (
          select
            sum(f.billable_size)
          from
            inv.inv_files f
          where
            f.inv_object_id=o.id
        )
      from
        inv.inv_objects o
      inner join inv.inv_collections_inv_objects icio
        on icio.inv_object_id = o.id
      inner join inv.inv_collections c
        on icio.inv_collection_id = c.id
      inner join inv.inv_files f
        on f.inv_object_id = o.id
      where f.pathname = ?
        and source = 'producer'
        and c.mnemonic = ?
      order by o.id asc
      limit 50;
    }
    run_query(
      sql: sql,
      params: [file, coll],
      title: "Object(s) by Filename: #{file} in #{coll}",
      headers: ['Object Id','Ark', 'Title', 'Version', 'Coll Id', 'Collection', 'File Count', 'Billable Size'],
      types: ['', 'ark', '', '', 'coll', 'mnemonic', 'dataint', 'dataint']
    )
  end

  def objects_by_file
    file = "producer/#{params['file'].strip}"
    sql = %{
      select
        o.id,
        o.ark,
        o.erc_what,
        o.version_number,
        c.id,
        c.mnemonic,
        (
          select
            count(f.id)
          from
            inv.inv_files f
          where
            f.inv_object_id=o.id
        ),
        (
          select
            sum(f.billable_size)
          from
            inv.inv_files f
          where
            f.inv_object_id=o.id
        )
      from
        inv.inv_objects o
      inner join inv.inv_collections_inv_objects icio
        on icio.inv_object_id = o.id
      inner join inv.inv_collections c
        on icio.inv_collection_id = c.id
      inner join inv.inv_files f
        on f.inv_object_id = o.id
      where f.pathname = ?
        and source = 'producer'
      order by o.id asc
      limit 50;
    }
    run_query(
      sql: sql,
      params: [file],
      title: "Object(s) by Filename: #{file}",
      headers: ['Object Id','Ark', 'Title', 'Version', 'Coll Id', 'Collection', 'File Count', 'Billable Size'],
      types: ['', 'ark', '', '', 'coll', 'mnemonic', 'dataint', 'dataint']
    )
  end

  def files_non_ascii
    subsql = %{
      select
        f.id
      from
        inv.inv_files f
      where
        f.pathname <> CONVERT(f.pathname USING ASCII)
      limit 20;
    }

    ids = run_subquery(sql: subsql)
    qs = []
    ids.each do|row|
      qs.push('?')
    end

    sql = %{
      select
        f.pathname,
        o.id,
        o.ark,
        c.id,
        c.mnemonic
      from
        inv.inv_files f
      inner join inv.inv_objects o
        on f.inv_object_id = o.id
      inner join inv.inv_collections_inv_objects co
        on co.inv_object_id = o.id
      inner join inv.inv_collections c
        on c.id = co.inv_collection_id and o.id = co.inv_object_id
      where
        f.id in (#{qs.join(',')})
    }
    run_query(
      sql: sql,
      params: ids,
      title: 'Files with Non Ascii Path (Max 20)',
      headers: ['File Path', 'Object Id', 'Ark', 'Collection Id', 'Collection Name'],
      types: ['', '', 'ark', '', 'mnemonic']
    )
  end

  def owners
    sql = %{
      select
        o.inv_owner_id,
        own.name as name,
        count(o.id) total
      from
        inv.inv_objects o
      inner join inv.inv_owners own
        on o.inv_owner_id = own.id
      group by o.inv_owner_id, own.name
      order by o.inv_owner_id
    }
    run_query(
      sql: sql,
      params: [],
      title: 'Counts by Owner',
      headers: ['Owner Id','Owner', 'Object Count'],
      types: ['own', '', 'dataint']
    )
  end

  def collections
    sql = %{
      select
        c.id,
        c.mnemonic,
        c.name,
        count(o.id) total
      from
        inv.inv_collections c
      inner join inv.inv_collections_inv_objects co
        on c.id = co.inv_collection_id
      inner join inv.inv_objects o
        on o.id = co.inv_object_id
      group by c.id, c.mnemonic, c.name
      order by c.id
    }
    run_query(
      sql: sql,
      params: [],
      title: 'Counts by Collection',
      headers: ['Collection Id', 'Mnemonic', 'Name', 'Object Count'],
      types: ['coll', 'mnemonic', '', 'dataint']
    )
  end

  def owners_coll
    own = params[:own]
    sql = %{
      select
        c.id,
        c.mnemonic,
        count(co.inv_object_id)
      from
        inv.inv_collections c
      inner join inv.inv_collections_inv_objects co
        on c.id = co.inv_collection_id
      inner join inv.inv_objects o
        on o.id = co.inv_object_id
      where
        o.inv_owner_id = ?
      group by
        c.id,
        c.mnemonic
      order by
        c.id
    }
    run_query(
      sql: sql,
      params: [own],
      title: "Counts by Owner #{own}",
      headers: ['Collection Id', 'Collection Name', 'Object Count'],
      types: ['coll', 'mnemonic', 'dataint']
    )
  end

  def large_object
    subsql = %{
      select
        f.inv_object_id
      from
        inv.inv_files f
      group by
        f.inv_object_id
      having
        sum(f.billable_size) > 1073741824
      limit 50;
    }
    ids = run_subquery(sql: subsql)
    qs = []
    ids.each do|row|
      qs.push('?')
    end

    sql = %{
      select
        o.id,
        o.ark,
        c.id,
        c.mnemonic,
        (select count(f.id) from inv.inv_files f where f.inv_object_id = o.id),
        (select sum(f.billable_size) from inv.inv_files f where f.inv_object_id = o.id)
      from
        inv.inv_objects o
      inner join inv.inv_collections_inv_objects icio
        on icio.inv_object_id = o.id
      inner join inv.inv_collections c
        on c.id = icio.inv_collection_id
      where
        o.id in (#{qs.join(',')});
    }
    run_query(
      sql: sql,
      params: ids,
      title: '50 Large Objects (need to paginate)',
      headers: ['Object Id','Ark', 'Collection Id', 'Collection', 'File Count', 'Billable Size'],
      types: ['', 'ark', '', 'mnemonic', 'dataint', 'dataint']
    )
  end

  def many_files
    subsql = %{
      select
        f.inv_object_id
      from
        inv.inv_files f
      group by
        f.inv_object_id
      having
        count(f.id) > 1000
      limit 50;
    }
    ids = run_subquery(sql: subsql)
    qs = []
    ids.each do|row|
      qs.push('?')
    end

    sql = %{
      select
        o.id,
        o.ark,
        c.id,
        c.mnemonic,
        (select count(f.id) from inv.inv_files f where f.inv_object_id = o.id),
        (select sum(f.billable_size) from inv.inv_files f where f.inv_object_id = o.id)
      from
        inv.inv_objects o
      inner join inv.inv_collections_inv_objects icio
        on icio.inv_object_id = o.id
      inner join inv.inv_collections c
        on c.id = icio.inv_collection_id
      where
        o.id in (#{qs.join(',')});
    }
    run_query(
      sql: sql,
      params: ids,
      title: 'Objects with Many Files (need to paginate)',
      headers: ['Object Id', 'Ark', 'Collection id', 'Collection', 'File Count', 'Billable Size'],
      types: ['', 'ark', '', 'mnemonic', 'dataint', 'dataint']
    )
  end

  def nodes
    sql = %{
      select
        number,
        description,
        count(inio.id) as total,
        sum(case when role ='primary' then 1 else 0 end),
        sum(case when role ='secondary' then 1 else 0 end)
      from
        inv.inv_nodes n
      inner join inv.inv_nodes_inv_objects inio
        on n.id = inio.inv_node_id
      group by number, description
      order by
        total desc;
    }
    run_query(
      sql: sql,
      params: [],
      title: 'Storage Nodes',
      headers: ['Node Number', 'Description', 'Total Obj', 'Primary', 'Secondary'],
      types: ['node', '', 'dataint', 'dataint', 'dataint']
    )
  end

  def coll_nodes
    node = params[:node]
    sql = %{
      select
        c.id,
        c.mnemonic,
        count(co.inv_object_id),
        sum(case when role ='primary' then 1 else 0 end),
        sum(case when role ='secondary' then 1 else 0 end)
      from
        inv.inv_collections c
        inner join inv.inv_collections_inv_objects co
          on c.id = co.inv_collection_id
        inner join inv.inv_nodes_inv_objects inio
          on co.inv_object_id = inio.inv_object_id
        inner join inv.inv_nodes n
          on n.id = inio.inv_node_id
        where
          n.number = ?
      group by
        c.id,
        c.mnemonic
      order by
        c.id
    }
    run_query(
      sql: sql,
      params: [node],
      title: "Storage Node #{node} Collections",
      headers: ['Collection Id', 'Collection Name', 'Total Obj', 'Primary', 'Secondary'],
      types: ['coll', 'mnemonic', 'dataint', 'dataint', 'dataint']
    )
  end

  def mime_types
    sql = %{
      select
        f.mime_type,
        count(*)
      from
        inv.inv_files f
      group by
        f.mime_type
      order by
        count(*) desc;
    }
    run_query(
      sql: sql,
      params: [],
      title: 'Mime Types (Producer and System)',
      headers: ['Mime Type', 'File Count'],
      types: ['mime', 'dataint']
    )
  end

  def mime_groups
    sql = %{
      select
         mime_group as g,
         mime_type as t,
         sum(count)
       from
         mime_use
       group by
         g,
         t
       union
       select
         distinct mime_group as g,
         '' as t,
         sum(count)
       from
         mime_use
       group by
         g,
         t
       order by
         g,
         t;
    }
    run_query(
      sql: sql,
      params: [],
      title: 'Mime Groups (Producer and System)',
      headers: ['Mime Group', 'Mime Type', 'File Count'],
      types: ['mime-group', 'mime', 'dataint']
    )
  end

  def coll_mime_types
    mime = params[:mime]
    sql = %{
      select
        c.id,
        c.mnemonic,
        count(*),
        sum(f.billable_size)
      from
        inv.inv_collections c
      inner join inv.inv_collections_inv_objects co
        on c.id = co.inv_collection_id
      inner join inv.inv_files f
        on f.inv_object_id = co.inv_object_id
      where
        f.mime_type = ?
      group by c.id, c.mnemonic
      order by c.id;
    }
    run_query(
      sql: sql,
      params: [mime],
      title: 'Mime Types by Collection',
      headers: ['Collection Id', 'Collection Name', 'File Count', 'Billable Size'],
      types: ['coll', 'mnemonic', 'dataint', 'dataint']
    )
  end

  def coll_details
    coll = params[:coll]
    sql = %{
      select
        f.mime_type,
        count(*),
        sum(f.billable_size)
      from
        inv.inv_collections c
      inner join inv.inv_collections_inv_objects co
        on c.id = co.inv_collection_id
      inner join inv.inv_files f
        on f.inv_object_id = co.inv_object_id
      where
        co.inv_collection_id = ?
      group by f.mime_type;
    }
    run_query(
      sql: sql,
      params: [coll],
      title: "Collection Details for #{coll}",
      headers: ['Mime Type', 'File Count', 'Billable Size'],
      types: ['', 'data', 'dataint']
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
    params: [],
    title: 'Title',
    headers: [],
    types: []
  )
    results = ActiveRecord::Base
      .connection
      .raw_connection
      .prepare(sql)
      .execute(*params)

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

  def run_subquery(
    sql: 'select 1',
    params: []
  )

    results = ActiveRecord::Base
      .connection
      .raw_connection
      .prepare(sql)
      .execute(*params)

    data = []
    if results.present?
      results.each do |row|
        data.push(row[0])
      end
    end
    data
  end
end
