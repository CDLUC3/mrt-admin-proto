class QueryController < ApplicationController

  def objects_query(where, params, title)
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
      where
    } + where +
    %{
      order by o.id asc
      limit 20;
    }
    run_query(
      sql: sql,
      params: params,
      title: title,
      headers: ['Object Id','Ark', 'Title', 'Version', 'Coll Id', 'Collection', 'File Count', 'Billable Size'],
      types: ['', 'ark', '', '', 'coll', 'mnemonic', 'dataint', 'dataint']
    )
  end


  def objects
    ark = "#{params['ark'].strip}%"
    objects_query('o.ark like ?', [ark], "Object(s) by Ark: #{ark}")
  end

  def objects_by_title
    title = "%#{params['title'].strip}%"
    objects_query('o.erc_what like ?', [title], "Object(s) by Title: #{title}")
  end

  def objects_by_author
    author = "%#{params['author'].strip}%"
    objects_query('o.erc_who like ?', [author], "Object(s) by Author: #{author}")
  end

  def files_query(where, params, title)
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
      where
    } + where +
    %{
      order by o.id asc
      limit 50;
    }
    run_query(
      sql: sql,
      params: params,
      title: title,
      headers: ['Object Id','Ark', 'Title', 'Version', 'Coll Id', 'Collection', 'File Count', 'Billable Size'],
      types: ['', 'ark', '', '', 'coll', 'mnemonic', 'dataint', 'dataint']
    )
  end

  def objects_by_file_coll
    file = "producer/#{params['file'].strip}"
    coll = params['coll'].strip
    files_query(
      "f.pathname = ? and source = 'producer' and c.mnemonic = ?",
      [file, coll],
      "Object(s) by Filename/Coll: #{file} in #{coll}"
    )
  end

  def objects_by_file
    file = "producer/#{params['file'].strip}"
    files_query(
      "f.pathname = ? and source = 'producer'",
      [file],
      "Object(s) by Filename: #{file}"
    )
  end

  # This query is too slow to run against millions of records
  def files_non_ascii
    subsql = %{
      select
        f.inv_object_id
      from
        inv.inv_files f
      where
        f.pathname <> CONVERT(f.pathname USING ASCII)
      limit 1;
    }

    ids = run_subquery(sql: subsql)
    qs = []
    ids.each do|row|
      qs.push('?')
    end

    objects_query("o.id in (#{qs.join(',')})", ids, "Objects with Files with Non Ascii Path (Max 20)")
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

    objects_query("o.id in (#{qs.join(',')})", ids, "50 Large Objects (need to paginate)")
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
    objects_query("o.id in (#{qs.join(',')})", ids, "Objects with Many Files (need to paginate)")
  end

  def owners
    sql = %{
      select
        ogroup,
        inv_owner_id as owner_id,
        own_name,
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
      title: 'File Counts by Owner',
      headers: ['Group', 'Owner Id','Owner', 'File Count', 'Billable Size'],
      types: ['ogroup', 'own', 'name', 'dataint', 'dataint'],
      filterCol: 2
    )
  end

  def owners_obj
    sql = %{
      select
        ogroup,
        collection_name,
        sum(count_objects) as count_objects
      from
        owner_collections_objects
      group by
        ogroup,
        collection_name
      union
      select
        ogroup,
        max('-- Total --') as collection_name,
        sum(count_objects) as count_objects
      from
        owner_collections_objects
      group by
        ogroup
      union
      select
        max('ZZZ') as ogroup,
        max('-- Grand Total --') as collection_name,
        sum(count_objects) as count_objects
      from
        owner_collections_objects
      order by
        ogroup,
        collection_name
    }
    run_query(
      sql: sql,
      params: [],
      title: 'Object Counts by Owner',
      headers: ['Group', 'Collection', 'Object Count'],
      types: ['ogroup', 'name', 'dataint'],
      filterCol: 1
    )
  end

  def collections
    sql = %{
      select
        ogroup,
        inv_collection_id,
        mnemonic,
        collection_name,
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
        sum(count_files) files,
        sum(billable_size) size
      from
        owner_coll_mime_use_details
      order by ogroup, collection_name
    }
    run_query(
      sql: sql,
      params: [],
      title: 'File Counts by Collection',
      headers: ['Group', 'Collection Id', 'Mnemonic', 'Name', 'File Count', 'Billable Size'],
      types: ['ogroup', 'coll', 'mnemonic', 'name', 'dataint', 'dataint'],
      filterCol: 3
    )
  end

  def coll_invoices
    # Fiscal year to report on.  Starting with FY2019, there are significant changes to the rate and adjustments for charge backs.
    fy = params[:fy].to_i

    # FY Start Date
    dstart = "#{fy}-07-01"

    # FY end date
    dend = "#{fy+1}-07-01"

    # As of allows you to test the pro-rating logic by using only a portion of data for a FY
    as_of = params.key?(:as_of) ? params[:as_of] : dend

    # Compute the last day in a FY (at or before the as_of date) for which records exist
    sql = %{
      select
        max(billing_totals_date)
      from
        daily_billing
      where
        billing_totals_date <= ?
      and
        billing_totals_date < ?
    }
    res = run_subquery(sql: sql, params: [as_of, dend])

    # YTD year to date date determined by the last available billing record
    dytd = res[0].to_s;

    # Determine if the fiscal year is in the past
    fypast = (Time.new.strftime('%Y-%m-%d') >= dend)

    # Compute the charge rate.  Before FY19: $650/TB.  After: $150/TB.
    # rate = (dend <= '2019-07-01') ? 0.000000000001780822 : 0.000000000000410959
    # Using our published rate: https://github.com/CDLUC3/mrt-doc/wiki/Policies-and-Procedures#pricing
    rate = (dend <= '2019-07-01') ? 0.00000000000178 : 0.000000000000410959

    # Format the annual rate to 2 digits of precision
    annrate = (rate * 1_000_000_000_000 * 365).to_i

    sqlfrag = %{
      /*
        The following query fragment will be used 3 times to create 3 levels of groupings.

        Compute usage at the campus/owner/collection level.
        - All Merritt objects have a collection and an owner object.
        - Generally, all objects in a collection have the same owner.
        - Some system collections have objects with differing ownership.
        As of June 2020, 37 "owner" objects exist in Merritt.
        "Campus" or "ogroup" is a logical grouping of the 37 objects to the 10 UC campuses + CDL.
      */
      select
        /* Select the query parameters to make them accessible to other calculations*/
        ? as dstart,
        ? as dend,
        ? as dytd,
        ? as rate,

        ogroup                          /* campus */,
        own_name                        /* Merritt ownership object.*/,
        inv_owner_id,
        collection_name,
        (
          select
            ifnull(avg(billable_size), 0)
          from
            daily_billing db
          where
            c.inv_collection_id = db.inv_collection_id
          and
            c.inv_owner_id = db.inv_owner_id
          and
            billing_totals_date = dstart
        ) as start_size                  /* usage on FY start date */,
        (
          select
            ifnull(avg(billable_size), 0)
          from
            daily_billing db
          where
            c.inv_collection_id = db.inv_collection_id
          and
            c.inv_owner_id = db.inv_owner_id
          and
            billing_totals_date = dytd
        ) as ytd_size                    /* usage on YTD date */,
        (
          select
            ifnull(avg(billable_size), 0)
          from
            daily_billing db
          where
            c.inv_collection_id = db.inv_collection_id
          and
            c.inv_owner_id = db.inv_owner_id
          and
            billing_totals_date = dend
        ) as end_size                    /* usage on FY end date */,
        (
          select ytd_size - start_size
        ) as diff_size                   /* YTD collection growth */,
        (
          select
            count(billable_size)
          from
            daily_billing db
          where
            c.inv_collection_id = db.inv_collection_id
          and
            c.inv_owner_id = db.inv_owner_id
          and
            billing_totals_date >= dstart
          and
            billing_totals_date <= dytd
        ) as days_available               /* number of billing day records in database */,
        (
          select if(datediff(dend, dytd) = 0, 0, datediff(dend, dytd) - 1)
        ) as days_projected               /* number of days to "project" to the end of the FY*/,
        (
          select
            avg(billable_size)
          from
            daily_billing db
          where
            c.inv_collection_id = db.inv_collection_id
          and
            c.inv_owner_id = db.inv_owner_id
          and
            billing_totals_date >= dstart
          and
            billing_totals_date <= dytd
        ) as average_available            /* YTD average size */,
        (
          select
            ((average_available * days_available) + (ytd_size * days_projected)) / datediff(dend, dstart)
        ) as daily_average_projected      /* Projected average for the FY */,
        (
          select
            case
              /* exemptions only apply before FY19*/
              when dstart < '2019-07-01' then
                (
                  select
                    ifnull((
                      select
                        max(exempt_bytes)
                      from
                        billing_owner_exemptions be
                      where
                        be.inv_owner_id = c.inv_owner_id
                    ), 0)
                )
              else 0
            end
        ) as owner_exempt_bytes                  /* If before FY19, compute storage exemption per owner */
      from
        owner_collections c
    }

    sql = %{
      /*
        Select campus/owner/collection level.
      */
      select
        dstart,
        ogroup                          /* campus */,
        own_name                        /* Merritt ownership object.*/,
        collection_name,
        start_size                  /* usage on FY start date */,
        ytd_size                    /* usage on YTD date */,
        end_size                    /* usage on FY end date */,
        diff_size                   /* YTD collection growth */,
        days_available               /* number of billing day records in database */,
        days_projected               /* number of days to "project" to the end of the FY*/,
        average_available            /* YTD average size */,
        daily_average_projected      /* Projected average for the FY */,
        null as owner_exempt_bytes,
        null as unexempt_average_projected,
        null as cost,
        null as cost_adj
      from
      (
        #{sqlfrag}
      ) collq

      union

      /*
        Aggregated usage at the CAMPUS level.
        - Before FY19: invoices were produced at the "owner" level, but only a fraction (14 of 37) were sent.
          - Grandfathered content has been designated as "exempt".
          - Exemption totals are pulled from a separate table.
          - A $50 minimum is applied to each invoice.
        - FY19 and beyond: invoices will be produced at a "campus" level.
          - Each campus will receive 10TB of free storage -- this replaces the notion of "exempt" content.
      */
      select
        max(dstart) as dstart,
        ogroup,
        max('-- Total --') as own_name,
        max('-- Total --') as collection_name,
        sum(start_size) as start_size,
        sum(ytd_size) as ytd_size,
        sum(end_size) as end_size,
        sum(diff_size) as end_size,
        null as days_available,
        max(days_projected) as days_projected,
        null as average_available,
        sum(daily_average_projected) as daily_average_projected,
        null as owner_exempt_bytes,
        null as unexempt_average_projected,
        (
          select
            case
              /* Before FY19, exemptions apply */
              when dstart < '2019-07-01' then null
              else sum(daily_average_projected)
            end * rate * 365
        ) as cost,
        (
          select
            case
              /* Before FY19, exemptions apply */
              when dstart < '2019-07-01' then null

              /* Starting in FY19, each campus receives 10TB of free storage */
              when sum(daily_average_projected) < 10000000000000 then 0
              else sum(daily_average_projected) - 10000000000000
            end * rate * 365
        ) as cost_adj
      from
      (
        #{sqlfrag}
      ) collq
      group by
        ogroup

      union

      /*
        Aggregated usage at the Merritt owner object level.
        - Before FY19: invoices were produced at the "owner" level, but only a fraction (14 of 37) were sent.
          - Grandfathered content has been designated as "exempt".
          - Exemption totals are pulled from a separate table.
          - A $50 minimum is applied to each invoice.
        - FY19 and beyond: invoices will be produced at a "campus" level.
          - Each campus will receive 10TB of free storage -- this replaces the notion of "exempt" content.
      */

      select
        max(dstart) as dstart,
        ogroup,
        own_name,
        max('-- Special Total --') as collection_name,
        sum(start_size) as start_size,
        sum(ytd_size) as ytd_size,
        sum(end_size) as end_size,
        sum(diff_size) as end_size,
        null as days_available,
        max(days_projected) as days_projected,
        null as average_available,
        sum(daily_average_projected) as daily_average_projected,
        max(owner_exempt_bytes) as owner_exempt_bytes,
        (
          select
            case
              /* Before FY19, $50 minimum per Merritt Owner */
              when dstart >= '2019-07-01' then null
              when (sum(daily_average_projected) - max(owner_exempt_bytes)) > 0
                then (sum(daily_average_projected) - max(owner_exempt_bytes))
              else 0
            end
        ) as unexempt_average_projected,
        (
          select
            case
              /* Before FY19, $50 minimum per Merritt Owner */
              when dstart >= '2019-07-01' then null
              when (sum(daily_average_projected) - max(owner_exempt_bytes)) * rate * 365 > 0
                then (sum(daily_average_projected) - max(owner_exempt_bytes)) * rate * 365
              else 0
            end
        ) as cost,
        (
          select
            case
              /* Before FY19, $50 minimum per Merritt Owner */
              when dstart >= '2019-07-01' then null
              when (sum(daily_average_projected) - max(owner_exempt_bytes)) * rate * 365 > 50
                then (sum(daily_average_projected) - max(owner_exempt_bytes)) * rate * 365
              when (sum(daily_average_projected) - max(owner_exempt_bytes)) * rate * 365 < 0
                then 0
              else 50
            end
        ) as cost_adj
      from
      (
        #{sqlfrag}
      ) collq
      group by
        ogroup,
        own_name

      order by
        ogroup,
        own_name,
        collection_name
    }
    run_query(
      sql: sql,
      # Note that the 4 parameters are passed 3 times since they are used in each of the 3 queries
      params: [
        dstart, dend, dytd, rate,
        dstart, dend, dytd, rate,
        dstart, dend, dytd, rate,
      ],
      title: "Invoice by Collection for FY#{fy}",
      headers: [
        '',
        'Group -- Campus.  This is an grouping applied to Merritt Owner objects within the billing script',
        'Owner -- Merritt Owner Object.  37 currently exist.',
        'Collection -- Merritt Collection Name',

        "FY Start -- Billable bytes on the first day of the FY",
        "FY YTD -- Billable bytes on the most recent reported day from the FY - applies when report is run mid year",
        "FY End -- Billable bytes on the last day of the FY",

        'Diff -- Bytes added since the start of the FY',
        'Days -- Days within the FY for which billable bytes were found for a collection',
        'Days Projected -- Number of days to the end of the FY.  The FY YTD amount will be presumed until the end of the FY',
        'Avg -- Average bytes found for the days in which content was found',

        'Daily Avg (Projected) (over whole year) -- Average bytes projected to the end of the year AND prorated for collections that were begun over the course of the FY',
        'Owner Exempt Bytes -- Pre FY2019 byte exemption for a Merritt Owner object',
        'Unexempt Avg -- Average bytes minus exemption bytes for a Merritt Owner object',

        "Cost/TB #{annrate} -- Cumulative daily storage cost for the entire fiscal year",
        "Adjusted Cost -- Adjusted cost. Before FY2019, all owners were assessed a $50 minimum charge.  Begining in FY2019, 10TB of complimentary storage are available to each campus."
      ],
      types: [
        'na',
        '', 'name', 'name',

        'dataint', #fy start
        fypast ? 'na' : 'dataint', #ytd
        fypast ? 'dataint' : 'na', #fy end

        'dataint', #difference
        'dataint', #days
        fypast ? 'na' : 'dataint', #days projected
        'dataint', #average - particularly useful for partial year collections

        'dataint', #projected average
        dstart < '2019-07-01' ? 'dataint' : 'na', # exempt bytes
        dstart < '2019-07-01' ? 'dataint' : 'na', #unexempt average

        'money', #cost
        'money' #adj cost
      ],
      filterCol: 3
    )
  end

  def owners_coll
    own = params[:own]
    sql = %{
      select
        c.id,
        c.mnemonic,
        c.name,
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
      headers: ['Collection Id', 'Mnemonic', 'Collection Name', 'File Count', 'Billable Size'],
      types: ['coll', 'mnemonic', '', 'dataint', 'dataint']
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
      types: ['gmime', 'mime', 'dataint', 'dataint'],
      filterCol: 1
    )
  end

  def coll_mime_types
    mime = params[:mime]
    sql = %{
      select
        ogroup,
        inv_collection_id,
        mnemonic,
        collection_name,
        sum(count_files),
        sum(billable_size)
      from
        owner_coll_mime_use_details
      where
        mime_type = ?
      group by ogroup, inv_collection_id, mnemonic, collection_name
      union
      select
        max('ZZ'),
        max(0),
        max(''),
        max('-- Total --'),
        sum(count_files),
        sum(billable_size)
      from
        owner_coll_mime_use_details
      where
        mime_type = ?
      order by ogroup, collection_name;
    }
    run_query(
      sql: sql,
      params: [mime, mime],
      title: "Collection distribution for mime type #{mime}",
      headers: ['Group', 'Collection Id', 'Mnemonic', 'Collection Name', 'File Count', 'Billable Size'],
      types: ['ogroup', 'coll', 'mnemonic', 'name', 'dataint', 'dataint'],
      filterCol: 3
    )
  end

  def coll_mime_groups
    gmime = params[:gmime]
    sql = %{
      select
        ogroup,
        inv_collection_id,
        mnemonic,
        collection_name,
        sum(count_files),
        sum(billable_size)
      from
        owner_coll_mime_use_details
      where
        mime_group = ?
      group by ogroup, inv_collection_id, mnemonic, collection_name
      union
      select
        max('ZZ'),
        max(0),
        max(''),
        max('-- Total --'),
        sum(count_files),
        sum(billable_size)
      from
        owner_coll_mime_use_details
      where
        mime_group = ?
      order by ogroup, collection_name;
    }
    run_query(
      sql: sql,
      params: [gmime, gmime],
      title: "Collection distribution for mime group #{gmime}",
      headers: ['Group', 'Collection Id', 'Mnemonic', 'Collection Name', 'File Count', 'Billable Size'],
      types: ['ogroup', 'coll', 'mnemonic', 'name', 'dataint', 'dataint'],
      filterCol: 3
    )
  end

  def coll_details
    coll = params[:coll]
    sql = %{
      select
        mime_group,
        mime_type,
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
      headers: ['Mime Group', 'Mime Type', 'File Count', 'Billable Size'],
      types: ['gmime', 'mime', 'dataint', 'dataint'],
      filterCol: 1
    )
  end

  def group_details
    ogroup = params[:ogroup]
    sql = %{
      select
        mime_group,
        mime_type,
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
      headers: ['Mime Group', 'Mime Type', 'File Count', 'Billable Size'],
      types: ['gmime', 'mime', 'dataint', 'dataint'],
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
