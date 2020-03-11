Rails.application.routes.draw do
  get '', to: redirect('/menu/index')
  get 'menu/index'
  get 'query/test'
  get 'query/large_object'
  get 'query/many_files'
  get 'query/nodes'
  get 'query/coll_nodes'
  get 'query/coll_nodes_by_node'
  get 'query/mime_types'
  get 'query/coll_mime_types'
  # For details on the DSL available within this file, see https://guides.rubyonrails.org/routing.html
end
