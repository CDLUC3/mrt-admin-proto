class MerrittObject < ApplicationRecord
  sql = "Select * from inv.inv_objects limit 10"
  records_array = ActiveRecord::Base.connection.execute(sql)
end
