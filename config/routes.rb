Rails.application.routes.draw do
  get '', to: redirect('/menu/index')
  get 'menu/index'
  get('query/objects' => 'query#objects')
  get('query/objects_by_title' => 'query#objects_by_title')
  get('query/objects_by_author' => 'query#objects_by_author')
  get('query/objects_by_file' => 'query#objects_by_file')
  get('query/objects_by_file_coll' => 'query#objects_by_file_coll')
  get 'query/large_object'
  get 'query/many_files'
  get 'query/nodes'
  get('query/coll_nodes/:node' => 'query#coll_nodes')
  get 'query/mime_types'
  get('query/coll_mime_types/:mime' => 'query#coll_mime_types')
  get 'query/owners'
  get 'query/collections'
  get('query/owners_coll/:own' => 'query#owners_coll')
  get 'query/files_non_ascii'
  get('query/coll_details/:coll' => 'query#coll_details')
  # For details on the DSL available within this file, see https://guides.rubyonrails.org/routing.html
end
