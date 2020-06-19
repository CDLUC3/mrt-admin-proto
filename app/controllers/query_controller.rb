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
        ogroup,
        inv_owner_id as owner_id,
        own_name,
        sum(count_objects) objects,
        sum(count_files) files,
        sum(billable_size) size
      from
        owner_coll_mime_use_details
      group by
        ogroup,
        owner_id,
        own_name
      union
      select
        ogroup,
        max(0) as owner_id,
        max('-- Total --') as own_name,
        sum(count_objects) objects,
        sum(count_files) files,
        sum(billable_size) size
      from
        owner_coll_mime_use_details
      group by
        ogroup
        union
      select
        max('ZZZ') as ogroup,
        max(0) as owner_id,
        max('-- Grand Total --') as own_name,
        sum(count_objects) objects,
        sum(count_files) files,
        sum(billable_size) size
      from
        owner_coll_mime_use_details
      order by
        ogroup,
        own_name
    }
    run_query(
      sql: sql,
      params: [],
      title: 'Counts by Owner',
      headers: ['Group', 'Owner Id','Owner', 'Object Count', 'File Count', 'Billable Size'],
      types: ['ogroup', 'own', 'name', 'dataint', 'dataint', 'dataint'],
      filterCol: 2
    )
  end

  def collections
    sql = %{
      select
        ogroup,
        inv_collection_id,
        mnemonic,
        collection_name,
        sum(count_objects) objects,
        sum(count_files) files,
        sum(billable_size) size
      from
        owner_coll_mime_use_details
      group by ogroup, inv_collection_id, mnemonic, collection_name
      union
      select
        ogroup,
        max(0),
        max(''),
        max('-- Total --') as collection_name,
        sum(count_objects) objects,
        sum(count_files) files,
        sum(billable_size) size
      from
        owner_coll_mime_use_details
      group by ogroup
      union
      select
        max('ZZZ') as ogroup,
        max(0),
        max(''),
        max('-- Grand Total --') as collection_name,
        sum(count_objects) objects,
        sum(count_files) files,
        sum(billable_size) size
      from
        owner_coll_mime_use_details
      order by ogroup, collection_name
    }
    run_query(
      sql: sql,
      params: [],
      title: 'Counts by Collection',
      headers: ['Group', 'Collection Id', 'Mnemonic', 'Name', 'Object Count', 'File Count', 'Billable Size'],
      types: ['ogroup', 'coll', 'mnemonic', 'name', 'dataint', 'dataint', 'dataint'],
      filterCol: 3
    )
  end

  def coll_invoices
    fy = params[:fy].to_i
    dstart = "#{fy}-07-01"
    dend = "#{fy+1}-07-01"
    dnow = Time.new.strftime("%Y-%m-%d")
    sql = %{
      select
        c.id,
        c.name,
        (
          select
            avg(billable_size)
          from
            daily_billing db
          where
            c.id = db.inv_collection_id
          and
            billing_totals_date = ?
        ),
        (
          select
            avg(billable_size)
          from
            daily_billing db
          where
            c.id = db.inv_collection_id
          and
            billing_totals_date = ?
        ),
        (
          select
            max(billable_size) - min(billable_size)
          from
            daily_billing db
          where
            c.id = db.inv_collection_id
          and
            billing_totals_date >= ?
          and
            billing_totals_date < ?
        ),
        (
          select
            avg(billable_size)
          from
            daily_billing db
          where
            c.id = db.inv_collection_id
          and
            billing_totals_date >= ?
          and
            billing_totals_date < ?
        ),
        (
          select
            count(billable_size)
          from
            daily_billing db
          where
            c.id = db.inv_collection_id
          and
            billing_totals_date >= ?
          and
            billing_totals_date < ?
        ),
        (
          select
            min(concat(?,?,?))
          from
            daily_billing db
          where
            c.id = db.inv_collection_id
          and
            billing_totals_date >= ?
          and
            billing_totals_date < ?
        )
      from
        inv.inv_collections c
      order by c.name
    }
    run_query(
      sql: sql,
      params: [dstart, dend, dstart, dend, dstart, dend, dstart, dend, dnow, dend, dstart, dstart, dend],
      title: "Invoice by Collection for FY#{fy}",
      headers: ['Collection Id', 'Name', 'FY Start', 'FY End', 'Diff', 'Avg', 'Days', 'Daily Avg'],
      types: ['coll', 'name', 'dataint', 'dataint', 'dataint', 'dataint', 'dataint', 'dataint']
    )
  end

  def owners_coll
    own = params[:own]
    sql = %{
      select
        c.id,
        c.mnemonic,
        c.name,
        sum(dmud.count_objects),
        sum(dmud.count_files),
        sum(dmud.billable_size)
      from
        inv.inv_collections c
      inner join daily_mime_use_details dmud
        on dmud.inv_collection_id = c.id
      where
        dmud.inv_owner_id = ?
      group by
        c.id,
        c.mnemonic,
        c.name
      order by
        c.name
    }
    run_query(
      sql: sql,
      params: [own],
      title: "Counts by Owner #{own}",
      headers: ['Collection Id', 'Mnemonic', 'Collection Name', 'Object Count', 'File Count', 'Billable Size'],
      types: ['coll', 'mnemonic', '', 'dataint', 'dataint', 'dataint']
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

  def mime_groups
    sql = %{
      select
        mime_group as g,
        mime_type as t,
        sum(count_files),
        sum(billable_size)
      from
         mime_use_details
       where
         source = 'producer'
      group by
         g,
         t
      union
      select
        mime_group as g,
        max('-- Total --') as t,
        sum(count_files),
        sum(billable_size)
      from
        mime_use_details
      where
        source = 'producer'
      group by
        g
      union
      select
        max('ZZ Merritt System') as g,
        max('-- Special Total --') as t,
        sum(count_files),
        sum(billable_size)
      from
        mime_use_details
      where
        source != 'producer'
      union
      select
        max('ZZZ') as g,
        max('-- Grand Total --') as t,
        sum(count_files),
        sum(billable_size)
      from
        mime_use_details
      order by
        g,
        t;
    }
    run_query(
      sql: sql,
      params: [],
      title: 'Mime Groups (Producer Files)',
      headers: ['Mime Group', 'Mime Type', 'File Count', 'Billable Size'],
      types: ['mime-group', 'mime', 'dataint', 'dataint'],
      filterCol: 1
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
        mime_group,
        mime_type,
        sum(count_objects),
        sum(count_files),
        sum(billable_size)
      from
        owner_coll_mime_use_details
      where
        source = 'producer'
      and
        inv_collection_id = ?
      group by
        mime_group, mime_type
      union
      select
        mime_group,
        max('-- Total --') as mime_type,
        sum(count_objects),
        sum(count_files),
        sum(billable_size)
      from
        owner_coll_mime_use_details
      where
        source = 'producer'
      and
        inv_collection_id = ?
      group by
        mime_group
      union
      select
        max('ZZ Merritt System') as mime_group,
        max('-- Special Total --') as mime_type,
        sum(count_objects),
        sum(count_files),
        sum(billable_size)
      from
        owner_coll_mime_use_details
      where
        source != 'producer'
      and
        inv_collection_id = ?
      union
      select
        max('ZZZ') as mime_group,
        max('-- Grand Total --') as mime_type,
        sum(count_objects),
        sum(count_files),
        sum(billable_size)
      from
        owner_coll_mime_use_details
      where
        inv_collection_id = ?
      order by
        mime_group, mime_type
    }
    run_query(
      sql: sql,
      params: [coll, coll, coll, coll],
      title: "Collection Details for #{coll}",
      headers: ['Mime Group', 'Mime Type', 'Object Count', 'File Count', 'Billable Size'],
      types: ['', 'mime', 'dataint', 'dataint', 'dataint'],
      filterCol: 1
    )
  end

  def group_details
    ogroup = params[:ogroup]
    sql = %{
      select
        mime_group,
        mime_type,
        sum(count_objects),
        sum(count_files),
        sum(billable_size)
      from
        owner_coll_mime_use_details
      where
        source = 'producer'
      and
        ogroup = ?
      group by
        mime_group, mime_type
      union
      select
        mime_group,
        max('-- Total --') as mime_type,
        sum(count_objects),
        sum(count_files),
        sum(billable_size)
      from
        owner_coll_mime_use_details
      where
        source = 'producer'
      and
        ogroup = ?
      group by
        mime_group
      union
      select
        max('ZZ Merritt System') as mime_group,
        max('-- Special Total --') as mime_type,
        sum(count_objects),
        sum(count_files),
        sum(billable_size)
      from
        owner_coll_mime_use_details
      where
        source != 'producer'
      and
        ogroup = ?
      union
      select
        max('ZZZ') as mime_group,
        max('-- Grand Total --') as mime_type,
        sum(count_objects),
        sum(count_files),
        sum(billable_size)
      from
        owner_coll_mime_use_details
      where
        ogroup = ?
      order by
        mime_group, mime_type
    }
    run_query(
      sql: sql,
      params: [ogroup, ogroup, ogroup, ogroup],
      title: "Collection Details for #{ogroup}",
      headers: ['Mime Group', 'Mime Type', 'Object Count', 'File Count', 'Billable Size'],
      types: ['', 'mime', 'dataint', 'dataint', 'dataint'],
      filterCol: 1
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
    types: [],
    filterCol: NIL
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
        data: data,
        filterCol: filterCol
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
